//! USBHS 零拷贝深队列 DMA pipe —— 只留框架，内存外部注入。
//!
//! # 设计
//! - **框架 vs 内存**：本模块只管队列状态机 + ISR 钩子。DMA 内存由调用方在
//!   启动时通过 `init_tx` / `init_rx` 注入一段 `&'static mut [DmaSlot]`。
//!   pipe 把这些 slot 的地址记进 ctx，DMA 直接打在调用方给的内存上。
//! - **零拷贝**：
//!   - TX(IN)：`TxPipe::alloc().await` 拿一块空闲 slot 的 `&mut [u8]`（`TxBuf`），
//!     调用方填好数据后 `TxBuf::submit(len)` 入队，ISR 自动续挂下一包。
//!   - RX(OUT)：`RxPipe::recv().await` 拿一块已收 slot 的 `&[u8]`（`RxBuf`），
//!     调用方处理完 `RxBuf::release()` 归还，ISR 自动补挂下一格。
//!   - 全程没有 `copy_nonoverlapping`：调用方拿到的是 DMA buffer 本体的借用。
//! - **TX/RX 两条独立队列**：`TxCtx` / `RxCtx` 是两个不同的类型，各自独立的 ring、
//!   各自的 free/ready 状态。一个端点只用其中一个（按 IN/OUT 方向）。
//! - **ISR 可达性**：USBHS 中断是无参的，ctx 必须能被 ISR 找到，所以仍放在本模块的
//!   `static` 注册表里；注入的只是 DMA buffer 本体，不是 ctx。
//!
//! # 并发模型
//! - ISR 单线程，不可重入；线程侧访问 ctx 必须在 `critical_section` 内（会屏蔽 USBHS IRQ）。
//! - `TxBuf` / `RxBuf` 是 RAII token：drop 时自动归还 slot，避免泄漏。
//! - `compiler_fence` 在 DMA 递交 / 取回前后保证内存序。

use core::future::poll_fn;
use core::sync::atomic::{compiler_fence, AtomicU16, AtomicU32, Ordering};
use core::task::Poll;

use ch32_metapac::usbhs::vals::{EpRxResponse, EpTog, EpTxResponse};

use super::Instance;

/// 端点数（含 EP0）。EP0 不走 ring。
pub const EP_N: usize = 16;

/// 单个 ring 的最大深度。注入的 slot 数可以 ≤ RING，按注入数量 `n` 工作。
pub const RING: usize = 8;

/// 调试开关：false 时 `init_tx` / `init_rx` 拒绝挂 ring，端点退回单缓冲 legacy 路径。
pub const RING_ENABLE: bool = true;

// ───────────────────────── 注入内存单元 ─────────────────────────

/// 一块 DMA 内存单元。调用方在启动时 `static` 一段 `[DmaSlot<SIZE>; N]` 然后
/// `init_tx` / `init_rx` 注入。对齐 4 字节，DMA 可直接访问。
#[repr(C, align(4))]
#[derive(Clone, Copy)]
pub struct DmaSlot<const SIZE: usize = 512> {
    pub data: [u8; SIZE],
}

impl<const SIZE: usize> DmaSlot<SIZE> {
    /// 全零构造（`const fn`，可放 `static`）。
    pub const fn new() -> Self {
        Self { data: [0; SIZE] }
    }
    fn addr(&self) -> u32 {
        self.data.as_ptr() as u32
    }
}

// ───────────────────────── TX / RX ctx ─────────────────────────

/// slot 在 TX 队列里的状态。
#[derive(Clone, Copy, PartialEq)]
enum TxSt {
    /// 空闲，可被 alloc。
    Free,
    /// 已 submit，等 ISR 发。
    Queued,
    /// 正在硬件里发。
    Armed,
    /// 被调用方 checkout（拿到 TxBuf），还没 submit。
    Alloc,
}

/// TX(IN) 一个端点的队列状态。ISR 与线程侧共享；线程侧须在 CS 内访问。
#[derive(Clone, Copy)]
pub struct TxCtx {
    addrs: [u32; RING],
    st: [TxSt; RING],
    lens: [u16; RING],
    /// 已 submit 的 FIFO（存 slot 下标）。
    q: [u8; RING],
    qh: u8,
    qt: u8,
    qlen: u8,
    /// 当前 Armed 的 slot（0xFF = 无），单独记便于 ISR 快速判断。
    armed: u8,
    n: u8,
    stopped: bool,
}

impl TxCtx {
    const fn empty() -> Self {
        Self {
            addrs: [0; RING],
            st: [TxSt::Free; RING],
            lens: [0; RING],
            q: [0; RING],
            qh: 0,
            qt: 0,
            qlen: 0,
            armed: 0xFF,
            n: 0,
            stopped: false,
        }
    }

    /// 当前占用槽位数（Armed + Queued + Alloc；Free 不算）。
    fn used(&self) -> u8 {
        let mut used = 0u8;
        let mut i = 0;
        while i < self.n {
            if self.st[i as usize] != TxSt::Free {
                used = used.saturating_add(1);
            }
            i += 1;
        }
        used
    }

    /// 找一个 Free slot 标记为 Alloc。调用方持 CS。
    fn alloc_slot(&mut self) -> Option<u8> {
        let mut i = 0;
        while i < self.n {
            if self.st[i as usize] == TxSt::Free {
                self.st[i as usize] = TxSt::Alloc;
                return Some(i);
            }
            i += 1;
        }
        None
    }

    /// ISR / 持 CS：把 Queued 队首挂到硬件。返回是否真的挂上。
    fn try_arm<T: Instance>(&mut self, index: usize) -> bool {
        if self.armed != 0xFF || self.qlen == 0 {
            return false;
        }
        let slot = self.q[self.qh as usize];
        self.qh = (self.qh + 1) % self.n;
        self.qlen -= 1;
        self.st[slot as usize] = TxSt::Armed;
        self.armed = slot;
        set_tx_dma::<T>(index, self.addrs[slot as usize], self.lens[slot as usize]);
        compiler_fence(Ordering::Release);
        ack_tx::<T>(index);
        true
    }
}

/// slot 在 RX 队列里的状态。
#[derive(Clone, Copy, PartialEq)]
enum RxSt {
    /// 已挂硬件等收。
    Armed,
    /// 已收，等调用方取。
    Ready,
    /// 被调用方 checkout（拿到 RxBuf），还没 release。
    Checkout,
    /// 空闲（未挂）。RX 一般常 Armed，Free 仅出现在刚 init / 暂停。
    Free,
}

/// RX(OUT) 一个端点的队列状态。ISR 与线程侧共享；线程侧须在 CS 内访问。
#[derive(Clone, Copy)]
pub struct RxCtx {
    addrs: [u32; RING],
    lens: [u16; RING],
    st: [RxSt; RING],
    /// 已收 Ready 的 FIFO（存 slot 下标）。
    ready: [u8; RING],
    rh: u8,
    rt: u8,
    rlen: u8,
    /// 当前 Armed 的 slot（0xFF = 无）。
    armed: u8,
    n: u8,
    stopped: bool,
}

impl RxCtx {
    const fn empty() -> Self {
        Self {
            addrs: [0; RING],
            lens: [0; RING],
            st: [RxSt::Free; RING],
            ready: [0; RING],
            rh: 0,
            rt: 0,
            rlen: 0,
            armed: 0xFF,
            n: 0,
            stopped: false,
        }
    }

    /// ISR / 持 CS：挂一个 Free slot 到硬件接收。返回是否挂上。
    fn try_arm<T: Instance>(&mut self, index: usize) -> bool {
        if self.armed != 0xFF {
            return false;
        }
        let mut i = 0;
        while i < self.n {
            if self.st[i as usize] == RxSt::Free {
                self.st[i as usize] = RxSt::Armed;
                self.armed = i;
                set_rx_dma::<T>(index, self.addrs[i as usize]);
                return true;
            }
            i += 1;
        }
        false
    }
}

// ───────────────────────── 静态注册表 ─────────────────────────

/// 仅由 USBHS ISR，或持有 critical_section 的线程访问。
static mut TX: [TxCtx; EP_N] = [TxCtx::empty(); EP_N];
static mut RX: [RxCtx; EP_N] = [RxCtx::empty(); EP_N];
/// bit i = 1 表示端点 i 已挂上 ring（ISR 热路径只读原子位，不进 CS）。
static RING_BITS: AtomicU16 = AtomicU16::new(0);
static EVT_RX: AtomicU32 = AtomicU32::new(0);
static EVT_TX: AtomicU32 = AtomicU32::new(0);

pub fn evt_rx() -> u32 {
    EVT_RX.load(Ordering::Relaxed)
}
pub fn evt_tx() -> u32 {
    EVT_TX.load(Ordering::Relaxed)
}

unsafe fn tx_mut(index: usize) -> &'static mut TxCtx {
    unsafe { &mut *core::ptr::addr_of_mut!(TX[index]) }
}
unsafe fn rx_mut(index: usize) -> &'static mut RxCtx {
    unsafe { &mut *core::ptr::addr_of_mut!(RX[index]) }
}

fn with_tx<R>(index: usize, f: impl FnOnce(&mut TxCtx) -> R) -> R {
    critical_section::with(|_| f(unsafe { tx_mut(index) }))
}
fn with_rx<R>(index: usize, f: impl FnOnce(&mut RxCtx) -> R) -> R {
    critical_section::with(|_| f(unsafe { rx_mut(index) }))
}

// ───────────────────────── 寄存器原语 ─────────────────────────

fn set_rx_dma<T: Instance>(index: usize, addr: u32) {
    if index == 0 {
        return;
    }
    T::dregs().ep_rx_dma(index - 1).write_value(addr);
}

fn set_tx_dma<T: Instance>(index: usize, addr: u32, len: u16) {
    if index == 0 {
        return;
    }
    T::dregs().ep_tx_dma(index - 1).write_value(addr);
    T::dregs().ep_t_len(index).write(|v| v.set_len(len));
}

fn clear_transfer<T: Instance>() {
    T::regs().int_fg().write(|v| v.set_transfer(true));
}

/// 一次 RMW：翻转 TOG 并设置 ACK/NAK（DMA 必须已经指到下一格）。
fn flip_rx_res<T: Instance>(index: usize, ack: bool) {
    T::dregs().ep_rx_ctrl(index).modify(|v| {
        v.set_mask_uep_r_tog(if v.mask_uep_r_tog() == EpTog::DATA0 {
            EpTog::DATA1
        } else {
            EpTog::DATA0
        });
        v.set_mask_uep_r_res(if ack {
            EpRxResponse::ACK
        } else {
            EpRxResponse::NAK
        });
    });
}

fn flip_tx_res<T: Instance>(index: usize, ack: bool) {
    T::dregs().ep_tx_ctrl(index).modify(|v| {
        v.set_mask_uep_t_tog(if v.mask_uep_t_tog() == EpTog::DATA0 {
            EpTog::DATA1
        } else {
            EpTog::DATA0
        });
        v.set_mask_uep_t_res(if ack {
            EpTxResponse::ACK
        } else {
            EpTxResponse::NAK
        });
    });
}

fn ack_tx<T: Instance>(index: usize) {
    T::dregs().ep_tx_ctrl(index).modify(|v| {
        v.set_mask_uep_t_res(EpTxResponse::ACK);
    });
}

fn set_buf_mod<T: Instance>(index: usize, on: bool) {
    T::dregs().ep_buf_mod().modify(|v| v.set_buf_mod(index, on));
}

// ───────────────────────── 注入 / 初始化 ─────────────────────────

/// 该端点是否已挂上 ring（ISR 热路径：只读原子位，不进 CS）。
pub fn is_ring(index: usize) -> bool {
    if index == 0 || index >= EP_N {
        return false;
    }
    RING_BITS.load(Ordering::Relaxed) & (1 << index) != 0
}

/// 给 IN 端点挂 TX 队列。`slots` 是调用方提供的 `'static mut` DMA 内存池。
/// 池大小 `slots.len()` 必须 ≤ `RING`，且 ≥ 2 才有意义。返回是否挂成功。
///
/// SAFETY：调用方保证 `slots` 在设备运行期间不被 alias（pipe 持有它的地址直到 reset）。
pub fn init_tx<T: Instance>(index: usize, slots: &'static mut [DmaSlot]) -> bool {
    if !RING_ENABLE || index == 0 || index >= EP_N || slots.len() > RING || slots.len() < 2 {
        return false;
    }
    let n = slots.len() as u8;
    let mut addrs = [0u32; RING];
    for (i, s) in slots.iter_mut().enumerate() {
        addrs[i] = s.addr();
    }
    with_tx(index, |c| {
        *c = TxCtx::empty();
        c.addrs = addrs;
        c.n = n;
    });
    RING_BITS.store(
        RING_BITS.load(Ordering::Relaxed) | (1 << index),
        Ordering::Release,
    );
    set_buf_mod::<T>(index, false);
    // TX 首包由 submit 时挂，这里不预挂。
    true
}

/// 给 OUT 端点挂 RX 队列。`slots` 是调用方提供的 `'static mut` DMA 内存池。
/// 池大小 `slots.len()` 必须 ≤ `RING`，且 ≥ 2 才有意义。返回是否挂成功。
///
/// SAFETY：调用方保证 `slots` 在设备运行期间不被 alias。
pub fn init_rx<T: Instance>(index: usize, slots: &'static mut [DmaSlot]) -> bool {
    if !RING_ENABLE || index == 0 || index >= EP_N || slots.len() > RING || slots.len() < 2 {
        return false;
    }
    let n = slots.len() as u8;
    let mut addrs = [0u32; RING];
    for (i, s) in slots.iter_mut().enumerate() {
        addrs[i] = s.addr();
    }
    with_rx(index, |c| {
        *c = RxCtx::empty();
        c.addrs = addrs;
        c.n = n;
    });
    RING_BITS.store(
        RING_BITS.load(Ordering::Relaxed) | (1 << index),
        Ordering::Release,
    );
    set_buf_mod::<T>(index, false);
    // 预挂第一格 RX，但保持 NAK，等 recv() 再 ACK。
    with_rx(index, |c| {
        c.try_arm::<T>(index);
    });
    true
}

fn reset_tx(c: &mut TxCtx) {
    c.st = [TxSt::Free; RING];
    c.qh = 0;
    c.qt = 0;
    c.qlen = 0;
    c.armed = 0xFF;
    c.stopped = false;
}

fn reset_rx(c: &mut RxCtx) {
    c.st = [RxSt::Free; RING];
    c.rh = 0;
    c.rt = 0;
    c.rlen = 0;
    c.armed = 0xFF;
    c.stopped = false;
}

pub fn reset(index: usize) {
    if index == 0 || index >= EP_N {
        return;
    }
    // 不知道方向，两边都清；只有挂了的那边 n>0 会真清。
    with_tx(index, reset_tx);
    with_rx(index, reset_rx);
}

pub fn reset_all() {
    for i in 0..EP_N {
        reset(i);
    }
}

/// 使能/复位端点时清 ring 状态并重新挂 RX DMA 基址；保持 NAK，等 recv() 再 ACK。
pub fn on_enable<T: Instance>(index: usize, enabled: bool, dir_in: bool) {
    reset(index);
    if index == 0 {
        return;
    }
    if dir_in {
        let n = with_tx(index, |c| c.n);
        if n == 0 {
            return;
        }
        // TX 首包由 submit 挂，这里不做事。
    } else {
        let (n, a0) = with_rx(index, |c| (c.n, c.addrs[0]));
        if n == 0 {
            return;
        }
        set_buf_mod::<T>(index, false);
        if enabled {
            set_rx_dma::<T>(index, a0);
        }
    }
}

// ───────────────────────── ISR 钩子 ─────────────────────────

/// ISR：OUT 完成。先挂下一格并清 `UIF_TRANSFER`，再返回是否需要 wake（ready 0→1）。
pub fn on_out<T: Instance>(index: usize, len: u16) -> bool {
    EVT_RX.store(EVT_RX.load(Ordering::Relaxed).wrapping_add(1), Ordering::Relaxed);
    let p = unsafe { rx_mut(index) };
    if p.n == 0 {
        clear_transfer::<T>();
        return false;
    }
    let was_empty = p.rlen == 0;
    // 把 armed slot 标记为 Ready，记长度，入 ready FIFO。
    let slot = if p.armed != 0xFF { p.armed } else { 0 };
    p.lens[slot as usize] = len;
    p.st[slot as usize] = RxSt::Ready;
    p.ready[p.rt as usize] = slot;
    p.rt = (p.rt + 1) % p.n;
    p.rlen = p.rlen.saturating_add(1);
    p.armed = 0xFF;

    compiler_fence(Ordering::Acquire);

    // 立刻补挂下一格，再 ACK/NAK，再清标志。
    let ack = p.try_arm::<T>(index);
    compiler_fence(Ordering::Release);
    flip_rx_res::<T>(index, ack);
    p.stopped = !ack;
    clear_transfer::<T>();
    was_empty
}

/// ISR：IN 完成。先挂下一包并清标志，再返回是否需要 wake（队列从满到非满）。
pub fn on_in<T: Instance>(index: usize) -> bool {
    EVT_TX.store(EVT_TX.load(Ordering::Relaxed).wrapping_add(1), Ordering::Relaxed);
    let p = unsafe { tx_mut(index) };
    if p.n == 0 {
        clear_transfer::<T>();
        return false;
    }
    // 上一包发完，armed slot 回 Free。
    let was_full = p.used() >= p.n;
    if p.armed != 0xFF {
        p.st[p.armed as usize] = TxSt::Free;
        p.armed = 0xFF;
    }
    // 续挂下一包。
    let armed = p.try_arm::<T>(index);
    if armed {
        // try_arm 已 ACK + 设 DMA；只翻 TOG（ACK 已设）。
        flip_tx_res::<T>(index, true);
    } else {
        flip_tx_res::<T>(index, false);
    }
    clear_transfer::<T>();
    was_full
}

// ───────────────────────── 零拷贝 TX API ─────────────────────────

/// 一块已 alloc 的 TX DMA buffer。调用方往里写数据，然后 `submit(len)` 入队。
/// drop 而不 submit 视为放弃，slot 自动回 Free。
pub struct TxBuf {
    slot: u8,
    ep: usize,
    /// DMA buffer 本体的借用。`'static` 因为 DMA 内存是注入的 static。
    data: &'static mut [u8],
}

impl TxBuf {
    /// 拿到这块 DMA buffer 的可写视图（零拷贝）。
    pub fn buf(&mut self) -> &mut [u8] {
        self.data
    }
    /// 已收/已写长度上限（slot 容量）。
    pub fn capacity(&self) -> usize {
        self.data.len()
    }
}

impl Drop for TxBuf {
    fn drop(&mut self) {
        // 没 submit 就 drop：归还 slot。
        let slot = self.slot;
        let ep = self.ep;
        with_tx(ep, |c| {
            if c.st[slot as usize] == TxSt::Alloc {
                c.st[slot as usize] = TxSt::Free;
            }
        });
    }
}

/// TX(IN) pipe 句柄。一个 IN 端点对应一个。零拷贝异步 API。
pub struct TxPipe<T: Instance> {
    pub index: usize,
    _t: core::marker::PhantomData<T>,
}

impl<T: Instance> TxPipe<T> {
    pub fn new(index: usize) -> Self {
        Self {
            index,
            _t: core::marker::PhantomData,
        }
    }

    /// 拿一块空闲 slot 的 DMA buffer（零拷贝）。队列满时等 IN 完成腾出槽。
    pub async fn alloc(&self) -> TxBuf {
        let index = self.index;
        poll_fn(|ctx| {
            super::EP_WAKERS[index].register(ctx.waker());
            let got = with_tx(index, |c| {
                if let Some(slot) = c.alloc_slot() {
                    let addr = c.addrs[slot as usize] as *mut u8;
                    let data = unsafe { core::slice::from_raw_parts_mut(addr, 512) };
                    Some(TxBuf { slot, ep: index, data })
                } else {
                    None
                }
            });
            match got {
                Some(b) => Poll::Ready(b),
                None => Poll::Pending,
            }
        })
        .await
    }

    /// 把 `buf` 入队等 ISR 发。`len` 必须 ≤ buffer 容量。消费 token。
    /// 成功即已入队（可能已 ACK 开传）。
    pub fn submit(&self, buf: TxBuf, len: u16) {
        let index = self.index;
        let slot = buf.slot;
        // 消解 token，手动归还流程接管。
        core::mem::forget(buf);
        with_tx(index, |c| {
            c.lens[slot as usize] = len;
            c.st[slot as usize] = TxSt::Queued;
            c.q[c.qt as usize] = slot;
            c.qt = (c.qt + 1) % c.n;
            c.qlen += 1;
            c.try_arm::<T>(index);
        });
    }

    /// 调试：当前占用槽位数（Armed+Queued+Alloc）。
    pub fn used(&self) -> u8 {
        with_tx(self.index, |c| c.used())
    }
    pub fn capacity(&self) -> u8 {
        with_tx(self.index, |c| c.n)
    }
}

// ───────────────────────── 零拷贝 RX API ─────────────────────────

/// 一块已收的 RX DMA buffer。调用方读数据，然后 `release()` 归还。
/// drop 等价于 release（自动归还 + 补挂）。
pub struct RxBuf {
    slot: u8,
    ep: usize,
    /// DMA buffer 本体的只读借用。`'static` 因为 DMA 内存是注入的 static。
    data: &'static [u8],
    len: u16,
}

impl RxBuf {
    /// 已收数据（零拷贝，长度由硬件 RX_LEN 决定）。
    pub fn data(&self) -> &[u8] {
        &self.data[..self.len as usize]
    }
    /// 已收长度。
    pub fn len(&self) -> usize {
        self.len as usize
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Drop for RxBuf {
    fn drop(&mut self) {
        // 没 release 就 drop：归还 slot（不补挂，等下次 recv / resume）。
        let slot = self.slot;
        let ep = self.ep;
        with_rx(ep, |c| {
            if c.st[slot as usize] == RxSt::Checkout {
                c.st[slot as usize] = RxSt::Free;
            }
        });
    }
}

/// RX(OUT) pipe 句柄。一个 OUT 端点对应一个。零拷贝异步 API。
pub struct RxPipe<T: Instance> {
    pub index: usize,
    _t: core::marker::PhantomData<T>,
}

impl<T: Instance> RxPipe<T> {
    pub fn new(index: usize) -> Self {
        Self {
            index,
            _t: core::marker::PhantomData,
        }
    }

    /// 取一块已收 slot 的 DMA buffer（零拷贝）。队列空时等 OUT 完成。
    /// 拿到后调用方必须 `release()`（或直接 drop）归还，否则槽位泄漏。
    pub async fn recv(&self) -> RxBuf {
        let index = self.index;
        poll_fn(|ctx| {
            super::EP_WAKERS[index].register(ctx.waker());
            let got = with_rx(index, |c| {
                if c.rlen == 0 {
                    // 队空：补挂 RX（首次读 / 环空 / 刚取走后），再等。
                    if c.armed == 0xFF {
                        let ack = c.try_arm::<T>(index);
                        compiler_fence(Ordering::Release);
                        flip_rx_res::<T>(index, ack);
                        c.stopped = !ack;
                    }
                    return None;
                }
                let slot = c.ready[c.rh as usize];
                c.rh = (c.rh + 1) % c.n;
                c.rlen -= 1;
                c.st[slot as usize] = RxSt::Checkout;
                let addr = c.addrs[slot as usize] as *const u8;
                let data = unsafe { core::slice::from_raw_parts(addr, 512) };
                Some(RxBuf {
                    slot,
                    ep: index,
                    data,
                    len: c.lens[slot as usize],
                })
            });
            match got {
                Some(b) => Poll::Ready(b),
                None => Poll::Pending,
            }
        })
        .await
    }

    /// 归还 `buf`，并补挂下一格 RX（保持流水）。消费 token。
    pub fn release(&self, buf: RxBuf) {
        let index = self.index;
        let slot = buf.slot;
        core::mem::forget(buf);
        with_rx(index, |c| {
            c.st[slot as usize] = RxSt::Free;
            if c.armed == 0xFF {
                let ack = c.try_arm::<T>(index);
                compiler_fence(Ordering::Release);
                flip_rx_res::<T>(index, ack);
                c.stopped = !ack;
            }
        });
    }

    /// 调试：已收待取包数。
    pub fn ready_count(&self) -> u8 {
        with_rx(self.index, |c| c.rlen)
    }
    pub fn capacity(&self) -> u8 {
        with_rx(self.index, |c| c.n)
    }
}

// ───────────────────────── 调试 ─────────────────────────

/// 调试：导出端点 ring 内部状态。IN 端点返回 TX 视图，OUT 端点返回 RX 视图。
/// `(n, used_or_rlen, armed, qlen, stopped)`。
pub fn dbg_state(index: usize) -> (u8, u8, u8, u8, u8) {
    if index == 0 || index >= EP_N {
        return (0, 0, 0, 0, 0);
    }
    let t = with_tx(index, |c| (c.n, c.used(), c.armed, c.qlen, c.stopped));
    if t.0 != 0 {
        return (t.0, t.1, t.2, t.3, t.4 as u8);
    }
    let r = with_rx(index, |c| (c.n, c.rlen, c.armed, 0, c.stopped));
    (r.0, r.1, r.2, r.3, r.4 as u8)
}
