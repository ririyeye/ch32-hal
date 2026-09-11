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

/// TX(IN) 方向走硬件双缓冲（`UEP_BUF_MOD` + `AUTO_TOG`，Phase 3）。
///
/// 两块缓冲预挂好、TOG 由硬件推进 ⇒ 清标志前**一次寄存器写都不需要**（RES 一直是 ACK），
/// 补货挪到清标志之后，有整整一包（~17us）的余量。实测：TX 侧 AUTO_TOG 可用
/// （`--bufmap-tx --bufmap-no-flip --bufmap-auto-tog` → A,B,A,B 交替，TOG_OK 全 1）。
const DBUF_TX: bool = false;

/// TX 双缓冲是否用硬件 `AUTO_TOG` 推进同步位。
/// true = 清标志前零写（余量最大，实测 +3.5us 不掉速）；false = 软件每包翻 TOG（窗口里 1 次写）。
/// 两者的峰值吞吐要用 `--pad-sweep`/`--metrics` 对比后再定。
const DBUF_TX_AUTO_TOG: bool = true;

/// TX(IN) 唤醒水位：free 队列涨到 `n / TX_WAKE_DIV` 格才唤醒生产者一次。
///
/// 1 = 每包都唤醒（老行为）。每唤醒一次，executor 都要把整个任务（含 `usb.run()`）
/// 重新轮询一遍，测速时那就是每包都要付的固定开销。设成 2 表示「攒到半个环再补」，
/// 生产者一次补满，唤醒次数减半，而队列仍有 `n/2` 的余量不会见底。
const TX_WAKE_DIV: u8 = 2;

/// 生产者该不该被唤醒：free 队列长度到水位了。
#[inline(always)]
fn tx_wake_water(n: u8) -> u8 {
    let w = n / TX_WAKE_DIV;
    if w == 0 { 1 } else { w }
}

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
    // ── 硬件双缓冲（BUF_MODE + AUTO_TOG）状态 ──
    /// 两个 DMA 寄存器里各挂着哪一格：`dbuf[0]`=本方向寄存器(TX_DMA/DATA0)，
    /// `dbuf[1]`=对方向寄存器(RX_DMA/DATA1)；0xFF = 这格没挂东西。
    dbuf: [u8; 2],
    /// 硬件下一次会用哪一块（0/1）。AUTO_TOG 由硬件推进，软件按"每完成一包翻一次"跟随。
    cur: u8,
    /// `T_LEN` 影子值（收发共用寄存器，只有变了才写，省一次窗口内写）。
    tlen: u16,
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
            dbuf: [0xFF, 0xFF],
            cur: 0,
            tlen: 0,
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
        // 复位/使能之后硬件那边是 NAK（`endpoint_set_enabled` 写的），
        // 所以这里必须是 true：补货补到 `cur` 那块时才记得把它改回 ACK。
        self.stopped = true;
        self.dbuf = [0xFF, 0xFF];
        self.cur = 0;
        self.tlen = 0;
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

// ───────────────────────── 诊断计数 ─────────────────────────
//
// 用来定位「一包要 20~30 µs，线上只要 10 µs」这类问题：
// - `DRY_*`：ISR 里队列空、只能给 NAK 的次数。它高说明瓶颈在应用侧喂不动 ring，
//   而不是 `INT_BUSY` 窗口；它接近 0 说明 ring 一直是满的，卡在别处。
// - `ISR_SUM/ISR_MAX`：ISR 从进到出的周期数。这就是 `UIF_TRANSFER` 有效、
//   硬件对后续 token 自动 NAK 的窗口长度。
// - `GAP_MAX`：两次 ISR 之间的最大间隔，即最坏情况下一包花了多久。

static ISR_N: AtomicU32 = AtomicU32::new(0);
static ISR_SUM: AtomicU32 = AtomicU32::new(0);
static ISR_MAX: AtomicU32 = AtomicU32::new(0);
static GAP_MAX: AtomicU32 = AtomicU32::new(0);
static LAST_ENTER: AtomicU32 = AtomicU32::new(0);
static DRY_RX: AtomicU32 = AtomicU32::new(0);
static DRY_TX: AtomicU32 = AtomicU32::new(0);

/// 青稞内核自带的 SysTick 计数器（`0xE000F000`，`CNT` 在 +8，64 位递增）。
/// 只取低 32 位做差分：HCLK=144MHz 时约 30 s 回绕一次，够用。
#[inline(always)]
fn cyc() -> u32 {
    unsafe { core::ptr::read_volatile(0xE000_F008 as *const u32) }
}

// ───────────────────── 实验旋钮：INT_BUSY 窗口垫片 ─────────────────────
//
// 用来回答「窗口还能长多少才会踩到主机的下一个 token」：在清 `UIF_TRANSFER` 前空转
// `ISR_PAD_*` 个 SysTick（≈55 ns/tick，见 `cyc()`），扫几档看吞吐从哪里崩。
// 崩点就是当前版本的真实余量。0 = 关（生产默认）。

static ISR_PAD_RX: AtomicU32 = AtomicU32::new(0);
static ISR_PAD_TX: AtomicU32 = AtomicU32::new(0);

/// 设置垫片：`rx` 作用于 `on_out`（OUT 方向），`tx` 作用于 `on_in`（IN 方向），单位 SysTick。
pub fn set_isr_pad(rx: u32, tx: u32) {
    ISR_PAD_RX.store(rx, Ordering::Relaxed);
    ISR_PAD_TX.store(tx, Ordering::Relaxed);
}

/// 读回当前垫片 `(rx, tx)`。
pub fn isr_pad() -> (u32, u32) {
    (
        ISR_PAD_RX.load(Ordering::Relaxed),
        ISR_PAD_TX.load(Ordering::Relaxed),
    )
}

/// 在窗口里空转 `n` 个 SysTick。n=0 时只有一次原子读 + 一次比较。
#[inline(always)]
fn pad_window(n: u32) {
    if n != 0 {
        let t0 = cyc();
        while cyc().wrapping_sub(t0) < n {
            core::hint::spin_loop();
        }
    }
}

/// 当前周期计数值（用于标定计数器频率：Δcounts / Δt）。
#[inline(always)]
pub fn cyc_raw() -> u32 {
    cyc()
}

/// 启动自由运行的周期计数器（`CTLR.STRE=1`，HCLK 递增，不开中断）。
pub fn metrics_init() {
    unsafe {
        let base = 0xE000_F000 as *mut u32;
        base.add(0).write_volatile(0); // CTLR 停
        base.add(1).write_volatile(0); // SR 清
        base.add(2).write_volatile(0); // CNT 低
        base.add(3).write_volatile(0); // CNT 高
        base.add(4).write_volatile(0xFFFF_FFFF); // CMP 低
        base.add(5).write_volatile(0xFFFF_FFFF); // CMP 高
        base.add(0).write_volatile(1); // CTLR.STRE=1，HCLK 递增
    }
}

/// ISR 入口调用：记录间隔并返回时间戳。
#[inline(always)]
pub fn metrics_enter() -> u32 {
    let t = cyc();
    let last = LAST_ENTER.load(Ordering::Relaxed);
    LAST_ENTER.store(t, Ordering::Relaxed);
    if last != 0 {
        // 用无符号回绕差分；跨过 0 也没关系。
        let gap = t.wrapping_sub(last);
        if gap > GAP_MAX.load(Ordering::Relaxed) {
            GAP_MAX.store(gap, Ordering::Relaxed);
        }
    }
    ISR_N.store(
        ISR_N.load(Ordering::Relaxed).wrapping_add(1),
        Ordering::Relaxed,
    );
    t
}

/// ISR 出口调用：累计 ISR 时长（含入口/出口的 trap 开销之外的部分）。
#[inline(always)]
pub fn metrics_exit(t0: u32) {
    let d = cyc().wrapping_sub(t0);
    ISR_SUM.store(
        ISR_SUM.load(Ordering::Relaxed).wrapping_add(d),
        Ordering::Relaxed,
    );
    if d > ISR_MAX.load(Ordering::Relaxed) {
        ISR_MAX.store(d, Ordering::Relaxed);
    }
}

/// 诊断快照：`(isr_n, isr_sum, isr_max, gap_max, dry_rx, dry_tx)`，单位都是周期数。
pub fn metrics() -> (u32, u32, u32, u32, u32, u32) {
    (
        ISR_N.load(Ordering::Relaxed),
        ISR_SUM.load(Ordering::Relaxed),
        ISR_MAX.load(Ordering::Relaxed),
        GAP_MAX.load(Ordering::Relaxed),
        DRY_RX.load(Ordering::Relaxed),
        DRY_TX.load(Ordering::Relaxed),
    )
}

/// 清空诊断计数（`isr_sum`/`gap_max` 这类是「区间统计」，读一次后清零更直观）。
pub fn metrics_clear() {
    ISR_N.store(0, Ordering::Relaxed);
    ISR_SUM.store(0, Ordering::Relaxed);
    ISR_MAX.store(0, Ordering::Relaxed);
    GAP_MAX.store(0, Ordering::Relaxed);
    DRY_RX.store(0, Ordering::Relaxed);
    DRY_TX.store(0, Ordering::Relaxed);
}

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

#[inline(always)]
fn set_tx_dma_reg<T: Instance>(index: usize, buf: u8, addr: u32) {
    // buf 0 = 本方向寄存器（TX_DMA / DATA0），1 = 对方向寄存器（RX_DMA / DATA1）
    if buf == 0 {
        T::dregs().ep_tx_dma(index - 1).write_value(addr);
    } else {
        T::dregs().ep_rx_dma(index - 1).write_value(addr);
    }
}

#[inline(always)]
fn set_t_len<T: Instance>(index: usize, len: u16) {
    T::dregs().ep_t_len(index).write(|v| v.set_len(len));
}

/// 双缓冲专用 CTRL 写：保留 `AUTO_TOG`（单缓冲那版会把它关掉）。
#[inline(always)]
fn write_tx_ctrl_auto<T: Instance>(index: usize, data1: bool, ack: bool) {
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
        v.set_t_tog_auto(true);
    });
}

/// 双缓冲的 CTRL 写：按 `DBUF_TX_AUTO_TOG` 选硬件自动翻还是软件翻。
#[inline(always)]
fn write_tx_ctrl_db<T: Instance>(index: usize, data1: bool, ack: bool) {
    if DBUF_TX_AUTO_TOG {
        write_tx_ctrl_auto::<T>(index, data1, ack);
    } else {
        write_tx_ctrl::<T>(index, data1, ack);
    }
}

/// 双缓冲是否需要补货：硬件下一个要用的那块、或再下一块空着。
#[inline(always)]
fn tx_needs_kick(p: &TxCtx) -> bool {
    p.dbuf[p.cur as usize] == 0xFF || p.dbuf[(p.cur ^ 1) as usize] == 0xFF
}

/// 双缓冲补货：优先补硬件下一个要用的那块（`cur`），再补再下一块。写寄存器、必要时
/// 写 `T_LEN`、必要时把 NAK 改回 ACK。**必须在清标志之后或 CS 里调用。**
fn tx_refill_db<T: Instance>(p: &mut TxCtx, index: usize) {
    for b in [p.cur, p.cur ^ 1] {
        if p.dbuf[b as usize] != 0xFF {
            continue;
        }
        compiler_fence(Ordering::Acquire);
        if p.qh == p.qt {
            break;
        }
        let slot = p.q[p.qh as usize];
        p.qh = wrap(p.qh);
        set_tx_dma_reg::<T>(index, b, p.addrs[slot as usize]);
        compiler_fence(Ordering::Release);
        p.dbuf[b as usize] = slot;
        if b == p.cur {
            p.armed = slot;
            // 硬件马上就要用这块：长度要对上，NAK 要改回 ACK
            if p.tlen != p.lens[slot as usize] {
                p.tlen = p.lens[slot as usize];
                set_t_len::<T>(index, p.tlen);
            }
            if p.stopped {
                p.stopped = false;
                write_tx_ctrl_db::<T>(index, p.cur == 1, true);
            }
        }
    }
}

/// 双缓冲 kick：把两块尽量挂满（等价于单缓冲的 kick_tx）。
fn kick_tx_db<T: Instance>(p: &mut TxCtx, index: usize) -> bool {
    tx_refill_db::<T>(p, index);
    if p.armed == 0xFF && !p.stopped {
        p.stopped = true;
        write_tx_ctrl_db::<T>(index, p.cur == 1, false);
    }
    p.armed != 0xFF
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
    p.tlen = p.lens[slot as usize];
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
    set_buf_mod::<T>(index, DBUF_TX);
    if DBUF_TX {
        // 双缓冲初值：TOG=DATA0、先 NAK，等 kick 挂满两块再 ACK
        T::dregs().ep_tx_ctrl(index).write(|v| {
            v.set_mask_uep_t_tog(EpTog::DATA0);
            v.set_mask_uep_t_res(EpTxResponse::NAK);
            v.set_t_tog_auto(DBUF_TX_AUTO_TOG);
        });
    }
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
        if DBUF_TX {
            // 两件事都会被上层清掉，必须在这里重新断言：
            // 1) `Bus::bus_reset()` 用 `ep_buf_mod().write_value(default())` 把 BUF_MODE 整片清零；
            // 2) `Bus::endpoint_set_enabled()` 会把 `t_tog_auto` 写回 false（单缓冲习惯）。
            // 少了任何一条，硬件就是单缓冲而软件按乒乓记账 —— 表现是"每格发两次、隔格丢"。
            set_buf_mod::<T>(index, true);
            T::dregs().ep_tx_ctrl(index).write(|v| {
                v.set_mask_uep_t_tog(EpTog::DATA0);
                v.set_mask_uep_t_res(EpTxResponse::NAK);
                v.set_t_tog_auto(DBUF_TX_AUTO_TOG);
            });
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
//
// 三条硬性顺序（改这里之前先读《CH32V307_USBHS_双工带宽问题说明》）：
//
// 1. **硬件动作必须在清 `UIF_TRANSFER` 之前**：`UEPn_RX_DMA`/`UEPn_TX_DMA`、`T_LEN`、
//    `R_TOG`/`T_TOG`、ACK/NAK 就是下一包的数据通路。标志一清，SIE 立刻会拿这套寄存器
//    去接/发下一包；没更新完就清，下一包会 DMA 覆盖上一包还没被应用读走的数据
//    （IN 侧则是把上一包原样重发）。这就是「清标志放太早」的危险所在。
// 2. **清标志要尽早**：`UC_INT_BUSY` 让 SIE 在 `UIF_TRANSFER` 有效期间对所有 token
//    自动 NAK，这段窗口就是纯粹的线上损失。
// 3. **纯软件记账放清标志之后**：搬 ready/free、改索引、计数、wake 都只碰 RAM，
//    放在窗口里只会白白拉长窗口。
//
// 于是一包的最短窗口 = 挂下一包要写的那几次 APB（OUT 2 次：DMA + CTRL；IN 3 次：
// DMA + T_LEN + CTRL）+ 1 次清标志，与「先清标志再挂 DMA」的 40MB+ 实验相比只多这几笔
// 寄存器写，但数据通路一定是自洽的。

/// ISR：OUT 完成。先挂下一格 + ACK/NAK，再清 `UIF_TRANSFER`，最后才记账。
/// 返回是否需要 wake（ready 0→1）。
#[inline(always)]
pub fn on_out<T: Instance>(index: usize, len: u16) -> bool {
    let p = unsafe { rx_mut(index) };
    if p.n == 0 {
        clear_transfer::<T>();
        return false;
    }

    // 先把要用的软件状态读出来（纯 RAM，不进窗口之后的部分）。
    compiler_fence(Ordering::Acquire);
    let has = p.fh != p.ft;
    let next = if has {
        p.free[p.fh as usize]
    } else {
        0xFF
    };
    let done = p.armed;
    let was_empty = p.rh == p.rt;
    let tog = p.tog;

    // ① 硬件动作：下一格地址 + 下一包 TOG + ACK/NAK。必须在清标志前完成。
    if has {
        set_rx_dma::<T>(index, p.addrs[next as usize]);
        compiler_fence(Ordering::Release);
        write_rx_ctrl::<T>(index, tog, true);
    } else {
        write_rx_ctrl::<T>(index, tog, false);
    }
    // ② 结束 INT_BUSY 窗口（垫片只在实验时非 0）。
    pad_window(ISR_PAD_RX.load(Ordering::Relaxed));
    clear_transfer::<T>();
    // ③ 软件记账。
    if has {
        p.armed = next;
        p.fh = wrap(p.fh);
        p.stopped = false;
        p.tog = !tog;
    } else {
        p.armed = 0xFF;
        p.stopped = true;
        DRY_RX.store(
            DRY_RX.load(Ordering::Relaxed).wrapping_add(1),
            Ordering::Relaxed,
        );
    }
    if done != 0xFF {
        p.lens[done as usize] = len;
        p.ready[p.rt as usize] = done;
        compiler_fence(Ordering::Release);
        p.rt = wrap(p.rt);
    }

    EVT_RX.store(
        EVT_RX.load(Ordering::Relaxed).wrapping_add(1),
        Ordering::Relaxed,
    );
    was_empty
}

/// ISR：IN 完成（硬件双缓冲版，Phase 3）。
///
/// 两块缓冲预先挂好、`AUTO_TOG` 由硬件推进 ⇒ 清标志前**什么都不用写**：
/// 下一包的 DMA 地址、长度、同步位、ACK 全就位，窗口里只剩「读 `INT_ST` + 清标志」。
/// 补货（写回刚空出来的那块寄存器、搬 free/记账、wake）全部在清标志之后，
/// 有整整一包（~17us）的余量。唯一例外：队列见底时要在清标志前把 RES 改成 NAK。
///
/// 返回是否需要 wake（free 队列到水位，生产者可以一次补一批）。
#[inline(always)]
fn on_in_db<T: Instance>(index: usize) -> bool {
    let p = unsafe { tx_mut(index) };
    if p.n == 0 {
        clear_transfer::<T>();
        return false;
    }

    let done = p.dbuf[p.cur as usize];
    p.cur ^= 1; // AUTO_TOG 已经把硬件推进到另一块
    let next_armed = p.dbuf[p.cur as usize] != 0xFF;

    // ① 窗口。用 AUTO_TOG 时 steady state 一次都不写；软件翻则每包写一次 CTRL。
    if !DBUF_TX_AUTO_TOG {
        write_tx_ctrl_db::<T>(index, p.cur == 1, next_armed);
        if !next_armed {
            p.stopped = true;
            DRY_TX.store(
                DRY_TX.load(Ordering::Relaxed).wrapping_add(1),
                Ordering::Relaxed,
            );
        }
    } else if !next_armed && !p.stopped {
        p.stopped = true;
        write_tx_ctrl_db::<T>(index, p.cur == 1, false);
        DRY_TX.store(
            DRY_TX.load(Ordering::Relaxed).wrapping_add(1),
            Ordering::Relaxed,
        );
    }
    // ② 结束 INT_BUSY 窗口
    clear_transfer::<T>();

    // ③ 记账 + 补货（窗口外）
    compiler_fence(Ordering::Acquire);
    let was_full = p.fh == p.ft;
    if done != 0xFF {
        p.free[p.ft as usize] = done;
        compiler_fence(Ordering::Release);
        p.ft = wrap(p.ft);
    }
    p.dbuf[p.cur as usize ^ 1] = 0xFF; // 刚空出来的那块（等下面的 refill 重新挂）
    tx_refill_db::<T>(p, index);
    p.armed = p.dbuf[p.cur as usize];

    let wake = qlen(p.fh, p.ft) >= tx_wake_water(p.n);
    EVT_TX.store(
        EVT_TX.load(Ordering::Relaxed).wrapping_add(1),
        Ordering::Relaxed,
    );
    let _ = was_full;
    wake
}

/// ISR：IN 完成（单缓冲版，`DBUF_TX=false` 时使用）。
///
/// **IN 方向故意不把记账挪到清标志之后**（与 `on_out` 不同）：实测同一块板子上
/// 「先记账再清标志」IN 30.9 MiB/s，「先清标志再记账」只有 27~28 MiB/s，
/// 而 OUT 方向恰好相反（22.8 → 42.5）。两个方向在这颗片子上的最优点不一样，
/// 这是量出来的，不是推出来的（见《CH32V307_USBHS_安全提速路径》§5）。
/// 安全不变量两边一致：清标志前的 DMA/LEN/TOG/ACK 一定已经就位。
///
/// 返回是否需要 wake（free 队列到水位，生产者可以一次补一批）。
#[inline(always)]
pub fn on_in<T: Instance>(index: usize) -> bool {
    if DBUF_TX {
        return on_in_db::<T>(index);
    }
    let p = unsafe { tx_mut(index) };
    if p.n == 0 {
        clear_transfer::<T>();
        return false;
    }

    compiler_fence(Ordering::Acquire);
    let has = p.qh != p.qt;
    let next = if has { p.q[p.qh as usize] } else { 0xFF };
    let done = p.armed;
    let tog = p.tog;

    // ① 硬件动作：下一包的 DMA/长度/TOG/ACK（清标志前必须全部就位）。
    if has {
        set_tx_dma::<T>(index, p.addrs[next as usize], p.lens[next as usize]);
        compiler_fence(Ordering::Release);
        write_tx_ctrl::<T>(index, tog, true);
    } else {
        write_tx_ctrl::<T>(index, tog, false);
    }
    // ② 记账（IN 方向上这一段留在清标志之前更快，见函数头）。
    if done != 0xFF {
        p.free[p.ft as usize] = done;
        compiler_fence(Ordering::Release);
        p.ft = wrap(p.ft);
    }
    if has {
        p.armed = next;
        p.qh = wrap(p.qh);
        p.stopped = false;
        p.tog = !tog;
    } else {
        p.armed = 0xFF;
        p.stopped = true;
        DRY_TX.store(
            DRY_TX.load(Ordering::Relaxed).wrapping_add(1),
            Ordering::Relaxed,
        );
    }
    // ③ 结束 INT_BUSY 窗口（垫片只在实验时非 0）。
    pad_window(ISR_PAD_TX.load(Ordering::Relaxed));
    clear_transfer::<T>();
    // free 涨到水位才叫醒生产者：一次补一批，减少 executor 轮询次数。
    let wake = qlen(p.fh, p.ft) >= tx_wake_water(p.n);

    EVT_TX.store(
        EVT_TX.load(Ordering::Relaxed).wrapping_add(1),
        Ordering::Relaxed,
    );
    wake
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
    ///
    /// 注意唤醒水位 `TX_WAKE_DIV`：Pending 之后不是「每空出一格就醒」，而是等
    /// free 攒到 `n / TX_WAKE_DIV` 格才醒一次（见该常量说明）。单包写入的调用方
    /// （`EndpointIn::write`）在深环上最多多等这个量级的包时间，测速这种「醒一次补一批」
    /// 的用法才是它的目标场景。
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
        if was_empty || tx_needs_kick(p) {
            with_tx(index, |c| {
                if tx_needs_kick(c) {
                    if DBUF_TX {
                        kick_tx_db::<T>(c, index);
                    } else if c.armed == 0xFF {
                        kick_tx::<T>(c, index);
                    }
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

/// TX 双缓冲内部状态：`(dbuf0, dbuf1, cur, tlen, armed, stopped)`。
pub fn tx_dbuf_state(index: usize) -> (u8, u8, u8, u16, u8, u8) {
    if index == 0 || index >= EP_N {
        return (0xFF, 0xFF, 0, 0, 0xFF, 1);
    }
    with_tx(index, |c| {
        (c.dbuf[0], c.dbuf[1], c.cur, c.tlen, c.armed, c.stopped as u8)
    })
}

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
