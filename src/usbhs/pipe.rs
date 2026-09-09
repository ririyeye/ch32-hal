//! USBHS 零拷贝深队列 DMA pipe —— 只留框架，内存外部注入。
//!
//! # 设计
//! - **框架 vs 内存**：本模块只管队列状态机 + ISR 钩子。DMA 内存由调用方在
//!   启动时通过 `init_tx` / `init_rx` 注入一段 `&'static mut [DmaSlot]`。
//!   pipe 把这些 slot 的地址记进 ctx，DMA 直接打在调用方给的内存上。
//! - **零拷贝**：
//!   - TX(IN)：`TxPipe::alloc().await` 拿一块空闲 slot 的 `&mut [u8]`（`TxBuf`），
//!     调用方填好数据后 `submit(len)` 入队，ISR 自动续挂下一包。
//!   - RX(OUT)：`RxPipe::recv().await` 拿一块已收 slot 的 `&[u8]`（`RxBuf`），
//!     调用方处理完 `release()` 归还，ISR 自动补挂下一格。
//! - **TX/RX 两条独立队列**：一个端点只用其中一个（按 IN/OUT 方向）。
//!
//! # 并发模型（单 Hart）
//! - ISR 不可重入。热路径是单生产者单消费者：线程不进 `critical_section`，
//!   避免屏蔽 USBHS IRQ、拉长 `INT_BUSY` 窗口。
//! - 索引用 `compiler_fence` 配对；只有空闲踢一脚（idle kick）和 TX `Drop`
//!   才进 CS（前者 ISR 已结束，后者极少走）。
//! - `tog` 由软件维护，ISR 对 `ep_*_ctrl` **只写不读**（一次 APB write）。
//! - `UIF_TRANSFER` 在环指针改完之后才清，避免下一包完成时队列还指着旧槽。

use core::future::poll_fn;
use core::sync::atomic::{compiler_fence, AtomicU16, AtomicU32, Ordering};
use core::task::Poll;

use ch32_metapac::usbhs::vals::{EpRxResponse, EpTog, EpTxResponse};

use super::Instance;

/// 端点数（含 EP0）。EP0 不走 ring。
pub const EP_N: usize = 16;

/// 单个 ring 的最大深度。注入的 slot 数可以 ≤ `RING`。
pub const RING: usize = 8;

/// 环形索引数组长度。容量 15，可装下 `RING` 个 slot；`head==tail` 表示空。
const Q: usize = 16;
const QMASK: u8 = 15;

/// 调试开关：false 时 `init_tx` / `init_rx` 拒绝挂 ring，端点退回单缓冲 legacy 路径。
pub const RING_ENABLE: bool = true;

#[inline(always)]
fn wrap(i: u8) -> u8 {
    (i + 1) & QMASK
}

#[inline(always)]
fn qlen(head: u8, tail: u8) -> u8 {
    tail.wrapping_sub(head) & QMASK
}

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

/// TX(IN) 一个端点的队列状态。
///
/// - `q`：线程生产、ISR 消费（已 submit）
/// - `free`：ISR 生产、线程消费（可 alloc）；TX `Drop` 走 CS 归还
#[derive(Clone, Copy)]
pub struct TxCtx {
    addrs: [u32; RING],
    lens: [u16; RING],
    q: [u8; Q],
    qh: u8,
    qt: u8,
    free: [u8; Q],
    fh: u8,
    ft: u8,
    armed: u8,
    /// 下一包要写进硬件的 DATA 翻转（false = DATA0）。
    tog: bool,
    n: u8,
    stopped: bool,
}

impl TxCtx {
    const fn empty() -> Self {
        Self {
            addrs: [0; RING],
            lens: [0; RING],
            q: [0; Q],
            qh: 0,
            qt: 0,
            free: [0; Q],
            fh: 0,
            ft: 0,
            armed: 0xFF,
            tog: false,
            n: 0,
            stopped: false,
        }
    }

    fn refill_free(&mut self) {
        let n = self.n;
        let mut i = 0u8;
        while i < n {
            self.free[i as usize] = i;
            i += 1;
        }
        self.fh = 0;
        self.ft = n;
        self.qh = 0;
        self.qt = 0;
        self.armed = 0xFF;
        self.tog = false;
        self.stopped = false;
    }

    fn used(&self) -> u8 {
        self.n.saturating_sub(qlen(self.fh, self.ft))
    }

    fn pop_free(&mut self) -> Option<u8> {
        compiler_fence(Ordering::Acquire);
        if self.fh == self.ft {
            return None;
        }
        let slot = self.free[self.fh as usize];
        self.fh = wrap(self.fh);
        Some(slot)
    }

    fn push_q(&mut self, slot: u8) -> bool {
        let empty = self.qh == self.qt;
        self.q[self.qt as usize] = slot;
        compiler_fence(Ordering::Release);
        self.qt = wrap(self.qt);
        empty
    }
}

/// RX(OUT) 一个端点的队列状态。
///
/// - `ready`：ISR 生产、线程消费
/// - `free`：线程生产、ISR 消费
#[derive(Clone, Copy)]
pub struct RxCtx {
    addrs: [u32; RING],
    lens: [u16; RING],
    ready: [u8; Q],
    rh: u8,
    rt: u8,
    free: [u8; Q],
    fh: u8,
    ft: u8,
    armed: u8,
    tog: bool,
    n: u8,
    stopped: bool,
}

impl RxCtx {
    const fn empty() -> Self {
        Self {
            addrs: [0; RING],
            lens: [0; RING],
            ready: [0; Q],
            rh: 0,
            rt: 0,
            free: [0; Q],
            fh: 0,
            ft: 0,
            armed: 0xFF,
            tog: false,
            n: 0,
            stopped: false,
        }
    }

    fn refill_free(&mut self) {
        let n = self.n;
        let mut i = 0u8;
        while i < n {
            self.free[i as usize] = i;
            i += 1;
        }
        self.fh = 0;
        self.ft = n;
        self.rh = 0;
        self.rt = 0;
        self.armed = 0xFF;
        self.tog = false;
        self.stopped = false;
    }

    fn pop_ready(&mut self) -> Option<u8> {
        compiler_fence(Ordering::Acquire);
        if self.rh == self.rt {
            return None;
        }
        let slot = self.ready[self.rh as usize];
        self.rh = wrap(self.rh);
        Some(slot)
    }

    fn push_free(&mut self, slot: u8) -> bool {
        let empty = self.fh == self.ft;
        self.free[self.ft as usize] = slot;
        compiler_fence(Ordering::Release);
        self.ft = wrap(self.ft);
        empty
    }
}

// ───────────────────────── 静态注册表 ─────────────────────────

/// 仅由 USBHS ISR，或单 Hart 上对应方向的生产者/消费者访问。
static mut TX: [TxCtx; EP_N] = [TxCtx::empty(); EP_N];
static mut RX: [RxCtx; EP_N] = [RxCtx::empty(); EP_N];
/// bit i = 1 表示端点 i 已挂上 ring（ISR 热路径只读原子位）。
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

#[inline(always)]
fn set_rx_dma<T: Instance>(index: usize, addr: u32) {
    T::dregs().ep_rx_dma(index - 1).write_value(addr);
}

#[inline(always)]
fn set_tx_dma<T: Instance>(index: usize, addr: u32, len: u16) {
    T::dregs().ep_tx_dma(index - 1).write_value(addr);
    T::dregs().ep_t_len(index).write(|v| v.set_len(len));
}

#[inline(always)]
fn clear_transfer<T: Instance>() {
    T::regs().int_fg().write(|v| v.set_transfer(true));
}

/// 一次 write：软件 TOG + ACK/NAK。不要 `modify()`（少一次 APB 读）。
#[inline(always)]
fn write_tx_ctrl<T: Instance>(index: usize, data1: bool, ack: bool) {
    T::dregs().ep_tx_ctrl(index).write(|v| {
        v.set_mask_uep_t_tog(if data1 {
            EpTog::DATA1
        } else {
            EpTog::DATA0
        });
        v.set_mask_uep_t_res(if ack {
            EpTxResponse::ACK
        } else {
            EpTxResponse::NAK
        });
        v.set_t_tog_auto(false);
    });
}

#[inline(always)]
fn write_rx_ctrl<T: Instance>(index: usize, data1: bool, ack: bool) {
    T::dregs().ep_rx_ctrl(index).write(|v| {
        v.set_mask_uep_r_tog(if data1 {
            EpTog::DATA1
        } else {
            EpTog::DATA0
        });
        v.set_mask_uep_r_res(if ack {
            EpRxResponse::ACK
        } else {
            EpRxResponse::NAK
        });
        v.set_r_tog_auto(false);
    });
}

fn set_buf_mod<T: Instance>(index: usize, on: bool) {
    T::dregs().ep_buf_mod().modify(|v| v.set_buf_mod(index, on));
}

/// 空闲时把队首挂上硬件。调用方须保证 `armed == 0xFF`（CS 或 ISR）。
fn kick_tx<T: Instance>(p: &mut TxCtx, index: usize) -> bool {
    compiler_fence(Ordering::Acquire);
    if p.qh == p.qt {
        p.stopped = true;
        write_tx_ctrl::<T>(index, p.tog, false);
        return false;
    }
    let slot = p.q[p.qh as usize];
    p.qh = wrap(p.qh);
    p.armed = slot;
    p.stopped = false;
    set_tx_dma::<T>(index, p.addrs[slot as usize], p.lens[slot as usize]);
    compiler_fence(Ordering::Release);
    write_tx_ctrl::<T>(index, p.tog, true);
    p.tog = !p.tog;
    true
}

fn kick_rx<T: Instance>(p: &mut RxCtx, index: usize) -> bool {
    compiler_fence(Ordering::Acquire);
    if p.fh == p.ft {
        p.stopped = true;
        write_rx_ctrl::<T>(index, p.tog, false);
        return false;
    }
    let slot = p.free[p.fh as usize];
    p.fh = wrap(p.fh);
    p.armed = slot;
    p.stopped = false;
    set_rx_dma::<T>(index, p.addrs[slot as usize]);
    compiler_fence(Ordering::Release);
    write_rx_ctrl::<T>(index, p.tog, true);
    p.tog = !p.tog;
    true
}

// ───────────────────────── 注入 / 初始化 ─────────────────────────

/// 该端点是否已挂上 ring（ISR 热路径：只读原子位）。
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
        c.refill_free();
    });
    RING_BITS.store(
        RING_BITS.load(Ordering::Relaxed) | (1 << index),
        Ordering::Release,
    );
    set_buf_mod::<T>(index, false);
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
        c.refill_free();
    });
    RING_BITS.store(
        RING_BITS.load(Ordering::Relaxed) | (1 << index),
        Ordering::Release,
    );
    set_buf_mod::<T>(index, false);
    true
}

fn reset_tx(c: &mut TxCtx) {
    if c.n != 0 {
        c.refill_free();
    }
}

fn reset_rx(c: &mut RxCtx) {
    if c.n != 0 {
        c.refill_free();
    }
}

pub fn reset(index: usize) {
    if index == 0 || index >= EP_N {
        return;
    }
    with_tx(index, reset_tx);
    with_rx(index, reset_rx);
}

pub fn reset_all() {
    for i in 0..EP_N {
        reset(i);
    }
}

/// 使能/复位端点时清 ring；RX 保持 NAK，等 `recv()` 再 ACK。
pub fn on_enable<T: Instance>(index: usize, _enabled: bool, dir_in: bool) {
    reset(index);
    if index == 0 {
        return;
    }
    if dir_in {
        let n = with_tx(index, |c| c.n);
        if n == 0 {
            return;
        }
    } else {
        let n = with_rx(index, |c| c.n);
        if n == 0 {
            return;
        }
        set_buf_mod::<T>(index, false);
    }
}

// ───────────────────────── ISR 钩子 ─────────────────────────

/// ISR：OUT 完成。先挂下一格并改完环，再清 `UIF_TRANSFER`。
/// 返回是否需要 wake（ready 0→1）。
#[inline(always)]
pub fn on_out<T: Instance>(index: usize, len: u16) -> bool {
    let p = unsafe { rx_mut(index) };
    if p.n == 0 {
        clear_transfer::<T>();
        return false;
    }

    compiler_fence(Ordering::Acquire);
    let has = p.fh != p.ft;
    let next = if has {
        p.free[p.fh as usize]
    } else {
        0xFF
    };
    if has {
        set_rx_dma::<T>(index, p.addrs[next as usize]);
        compiler_fence(Ordering::Release);
        write_rx_ctrl::<T>(index, p.tog, true);
        p.tog = !p.tog;
    } else {
        write_rx_ctrl::<T>(index, p.tog, false);
    }

    let done = p.armed;
    let was_empty = p.rh == p.rt;
    if done != 0xFF {
        p.lens[done as usize] = len;
        p.ready[p.rt as usize] = done;
        compiler_fence(Ordering::Release);
        p.rt = wrap(p.rt);
    }
    if has {
        p.armed = next;
        p.fh = wrap(p.fh);
        p.stopped = false;
    } else {
        p.armed = 0xFF;
        p.stopped = true;
    }
    clear_transfer::<T>();

    EVT_RX.store(
        EVT_RX.load(Ordering::Relaxed).wrapping_add(1),
        Ordering::Relaxed,
    );
    was_empty
}

/// ISR：IN 完成。先挂下一包并改完环，再清 `UIF_TRANSFER`。
/// 返回是否需要 wake（free 0→1，即队列从满到非满）。
#[inline(always)]
pub fn on_in<T: Instance>(index: usize) -> bool {
    let p = unsafe { tx_mut(index) };
    if p.n == 0 {
        clear_transfer::<T>();
        return false;
    }

    compiler_fence(Ordering::Acquire);
    let has = p.qh != p.qt;
    let next = if has { p.q[p.qh as usize] } else { 0xFF };
    if has {
        set_tx_dma::<T>(
            index,
            p.addrs[next as usize],
            p.lens[next as usize],
        );
        compiler_fence(Ordering::Release);
        write_tx_ctrl::<T>(index, p.tog, true);
        p.tog = !p.tog;
    } else {
        write_tx_ctrl::<T>(index, p.tog, false);
    }

    let done = p.armed;
    compiler_fence(Ordering::Acquire);
    let was_full = p.fh == p.ft;
    if done != 0xFF {
        p.free[p.ft as usize] = done;
        compiler_fence(Ordering::Release);
        p.ft = wrap(p.ft);
    }
    if has {
        p.armed = next;
        p.qh = wrap(p.qh);
        p.stopped = false;
    } else {
        p.armed = 0xFF;
        p.stopped = true;
    }
    clear_transfer::<T>();

    EVT_TX.store(
        EVT_TX.load(Ordering::Relaxed).wrapping_add(1),
        Ordering::Relaxed,
    );
    was_full
}

// ───────────────────────── 零拷贝 TX API ─────────────────────────

/// 一块已 alloc 的 TX DMA buffer。调用方往里写数据，然后 `submit(len)` 入队。
/// drop 而不 submit 视为放弃，slot 自动回 Free。
pub struct TxBuf {
    slot: u8,
    ep: usize,
    data: &'static mut [u8],
}

impl TxBuf {
    pub fn buf(&mut self) -> &mut [u8] {
        self.data
    }
    pub fn capacity(&self) -> usize {
        self.data.len()
    }
}

impl Drop for TxBuf {
    fn drop(&mut self) {
        let slot = self.slot;
        let ep = self.ep;
        // TX free 的正常生产者是 ISR；Drop 是第二条生产路径，必须进 CS。
        with_tx(ep, |c| {
            c.free[c.ft as usize] = slot;
            compiler_fence(Ordering::Release);
            c.ft = wrap(c.ft);
        });
    }
}

/// TX(IN) pipe 句柄。一个 IN 端点对应一个。
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

    /// 拿一块空闲 slot 的 DMA buffer。队列满时等 IN 完成腾出槽。
    pub async fn alloc(&self) -> TxBuf {
        let index = self.index;
        poll_fn(|ctx| {
            super::EP_WAKERS[index].register(ctx.waker());
            let p = unsafe { tx_mut(index) };
            match p.pop_free() {
                Some(slot) => {
                    let addr = p.addrs[slot as usize] as *mut u8;
                    let data = unsafe { core::slice::from_raw_parts_mut(addr, 512) };
                    Poll::Ready(TxBuf {
                        slot,
                        ep: index,
                        data,
                    })
                }
                None => Poll::Pending,
            }
        })
        .await
    }

    /// 把 `buf` 入队。队列原先为空时进 CS 踢一脚（流式满队列不进 CS）。
    pub fn submit(&self, buf: TxBuf, len: u16) {
        let index = self.index;
        let slot = buf.slot;
        core::mem::forget(buf);
        let p = unsafe { tx_mut(index) };
        p.lens[slot as usize] = len;
        let was_empty = p.push_q(slot);
        if was_empty {
            with_tx(index, |c| {
                if c.armed == 0xFF {
                    kick_tx::<T>(c, index);
                }
            });
        }
    }

    pub fn used(&self) -> u8 {
        let p = unsafe { tx_mut(self.index) };
        compiler_fence(Ordering::Acquire);
        p.used()
    }
    pub fn capacity(&self) -> u8 {
        unsafe { tx_mut(self.index) }.n
    }
}

// ───────────────────────── 零拷贝 RX API ─────────────────────────

/// 一块已收的 RX DMA buffer。调用方读数据，然后 `release()` 归还。
/// drop 等价于 release（自动归还 + 必要时补挂）。
pub struct RxBuf {
    slot: u8,
    ep: usize,
    data: &'static [u8],
    len: u16,
}

impl RxBuf {
    pub fn data(&self) -> &[u8] {
        &self.data[..self.len as usize]
    }
    pub fn len(&self) -> usize {
        self.len as usize
    }
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Drop for RxBuf {
    fn drop(&mut self) {
        // 只归还 free。补挂要写寄存器，走 `release()` / 下一次 `recv()`。
        unsafe { rx_mut(self.ep) }.push_free(self.slot);
    }
}

/// RX(OUT) pipe 句柄。一个 OUT 端点对应一个。
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

    /// 取一块已收 slot。队列空时等 OUT 完成。
    pub async fn recv(&self) -> RxBuf {
        let index = self.index;
        poll_fn(|ctx| {
            super::EP_WAKERS[index].register(ctx.waker());
            let p = unsafe { rx_mut(index) };
            if let Some(slot) = p.pop_ready() {
                let addr = p.addrs[slot as usize] as *const u8;
                let data = unsafe { core::slice::from_raw_parts(addr, 512) };
                return Poll::Ready(RxBuf {
                    slot,
                    ep: index,
                    data,
                    len: p.lens[slot as usize],
                });
            }
            if p.armed == 0xFF {
                with_rx(index, |c| {
                    if c.armed == 0xFF {
                        kick_rx::<T>(c, index);
                    }
                });
            }
            Poll::Pending
        })
        .await
    }

    /// 归还 `buf`，并在硬件空闲时补挂。
    pub fn release(&self, buf: RxBuf) {
        let index = self.index;
        let slot = buf.slot;
        core::mem::forget(buf);
        let p = unsafe { rx_mut(index) };
        let was_empty = p.push_free(slot);
        if was_empty || p.stopped {
            with_rx(index, |c| {
                if c.armed == 0xFF {
                    kick_rx::<T>(c, index);
                }
            });
        }
    }

    pub fn ready_count(&self) -> u8 {
        let p = unsafe { rx_mut(self.index) };
        compiler_fence(Ordering::Acquire);
        qlen(p.rh, p.rt)
    }
    pub fn capacity(&self) -> u8 {
        unsafe { rx_mut(self.index) }.n
    }
}

// ───────────────────────── 调试 ─────────────────────────

/// `(n, used_or_rlen, armed, qlen, stopped)`。
pub fn dbg_state(index: usize) -> (u8, u8, u8, u8, u8) {
    if index == 0 || index >= EP_N {
        return (0, 0, 0, 0, 0);
    }
    let t = with_tx(index, |c| {
        (c.n, c.used(), c.armed, qlen(c.qh, c.qt), c.stopped as u8)
    });
    if t.0 != 0 {
        return t;
    }
    let r = with_rx(index, |c| {
        (c.n, qlen(c.rh, c.rt), c.armed, 0, c.stopped as u8)
    });
    r
}
