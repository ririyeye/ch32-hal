//! Bulk 深队列 DMA ring（不和 BUF_MODE 硬件双缓冲叠用，避免硬件切缓冲和软件改指针打架）。
//! - OUT：ISR 记长度、RX_DMA 指到下一格、继续 ACK；环满 NAK。
//! - IN：深队列，发完一包 ISR 立刻挂下一包；队列空 NAK。
//! - EP0 与没拿到 ring 的端点仍走上游单包闩锁路径（本模块的 on_* 只被 ring 端点调用）。
//!
//! INT_BUSY：`UIF_TRANSFER` 置位期间 SIE 对所有 token 自动 NAK（不改 RES 寄存器）。
//! ISR 必须先写下一格 DMA + ACK/NAK，立刻清标志，再做计数/wake。
//! 线程侧访问 `EPS` 必须在 critical_section 里（会屏蔽 USBHS IRQ）；ISR 内不再进 CS。

use core::sync::atomic::{compiler_fence, AtomicU16, AtomicU32, Ordering};

use ch32_metapac::usbhs::vals::{EpRxResponse, EpTog, EpTxResponse};

use super::Instance;

pub const RING: usize = 8;
const EP_N: usize = 16;
const MAX_BULK: usize = 2;

/// 调试开关：false 时全部端点退回单缓冲 legacy 路径。
pub const RING_ENABLE: bool = true;

#[derive(Clone, Copy)]
#[repr(C, align(4))]
struct Dma512 {
    data: [u8; 512],
}

impl Dma512 {
    const fn zero() -> Self {
        Self { data: [0; 512] }
    }
}

#[derive(Clone, Copy)]
struct BulkEp {
    addrs: [u32; RING],
    lens: [u16; RING],
    n: u8,
    dir_in: bool,
    cons: u8,
    filled: u8,
    hw: [u8; 2],
    n_hw: u8,
    q: [u8; RING],
    qh: u8,
    qt: u8,
    qlen: u8,
    reserve: u8,
    stopped: bool,
}

impl BulkEp {
    const fn empty() -> Self {
        Self {
            addrs: [0; RING],
            lens: [0; RING],
            n: 0,
            dir_in: false,
            cons: 0,
            filled: 0,
            hw: [0xFF, 0xFF],
            n_hw: 0,
            q: [0; RING],
            qh: 0,
            qt: 0,
            qlen: 0,
            reserve: 0xFF,
            stopped: false,
        }
    }

    fn used(&self) -> u8 {
        self.filled
            .saturating_add(self.n_hw)
            .saturating_add(self.qlen)
            .saturating_add(if self.reserve == 0xFF { 0 } else { 1 })
    }

    fn free_slot(&self) -> Option<u8> {
        if self.n == 0 || self.used() as usize >= self.n as usize {
            return None;
        }
        for i in 0..self.n {
            if self.hw[0] == i || self.hw[1] == i || self.reserve == i {
                continue;
            }
            let mut in_q = false;
            for k in 0..self.qlen {
                if self.q[((self.qh + k) % self.n) as usize] == i {
                    in_q = true;
                    break;
                }
            }
            if !in_q {
                return Some(i);
            }
        }
        None
    }
}

/// 仅由 USBHS ISR，或持有 critical_section 的线程访问。
static mut EPS: [BulkEp; EP_N] = [BulkEp::empty(); EP_N];
static RING_BITS: AtomicU16 = AtomicU16::new(0);
static EVT_RX: AtomicU32 = AtomicU32::new(0);
static EVT_TX: AtomicU32 = AtomicU32::new(0);

pub fn evt_rx() -> u32 {
    EVT_RX.load(Ordering::Relaxed)
}
pub fn evt_tx() -> u32 {
    EVT_TX.load(Ordering::Relaxed)
}

/// SAFETY: USBHS ISR 不可重入；线程侧调用方必须在 critical_section 内。
unsafe fn ep_mut(index: usize) -> &'static mut BulkEp {
    unsafe { &mut *core::ptr::addr_of_mut!(EPS[index]) }
}

fn with_ep<R>(index: usize, f: impl FnOnce(&mut BulkEp) -> R) -> R {
    critical_section::with(|_| f(unsafe { ep_mut(index) }))
}

fn take_ring() -> Option<[u32; RING]> {
    critical_section::with(|_| {
        static mut NEXT: usize = 0;
        static mut RINGS: [[Dma512; RING]; MAX_BULK] = [[Dma512::zero(); RING]; MAX_BULK];
        unsafe {
            if NEXT >= MAX_BULK {
                return None;
            }
            let mut addrs = [0u32; RING];
            for i in 0..RING {
                addrs[i] = RINGS[NEXT][i].data.as_mut_ptr() as u32;
            }
            NEXT += 1;
            Some(addrs)
        }
    })
}

/// 该端点是否已挂上 ring（ISR 热路径：只读原子位，不进 CS）。
pub fn is_ring(index: usize) -> bool {
    if index == 0 || index >= EP_N {
        return false;
    }
    RING_BITS.load(Ordering::Relaxed) & (1 << index) != 0
}

/// 给端点挂深队列 ring（DMA 地址为静态 ring 槽）。
/// 返回是否真的拿到了 ring（最多 MAX_BULK 条，拿不到则调用方回退单缓冲）。
pub fn init<T: Instance>(index: usize, dir_in: bool) -> bool {
    if index == 0 || index >= EP_N {
        return false;
    }
    let Some(addrs) = take_ring() else {
        return false;
    };
    with_ep(index, |ep| {
        *ep = BulkEp::empty();
        ep.addrs = addrs;
        ep.n = RING as u8;
        ep.dir_in = dir_in;
    });
    RING_BITS.store(
        RING_BITS.load(Ordering::Relaxed) | (1 << index),
        Ordering::Release,
    );
    set_buf_mod::<T>(index, false);
    if !dir_in {
        set_rx_dma::<T>(index, addrs[0]);
    } else {
        set_tx_dma::<T>(index, addrs[0], 0);
    }
    true
}

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

fn reset_ep(p: &mut BulkEp) {
    p.cons = 0;
    p.filled = 0;
    p.hw = [0xFF, 0xFF];
    p.n_hw = 0;
    p.qh = 0;
    p.qt = 0;
    p.qlen = 0;
    p.reserve = 0xFF;
    p.stopped = false;
}

pub fn reset(index: usize) {
    if index == 0 || index >= EP_N {
        return;
    }
    with_ep(index, reset_ep);
}

pub fn reset_all() {
    for i in 0..EP_N {
        reset(i);
    }
}

/// 使能/复位端点时清 ring 状态并重新挂 RX DMA 基址；保持 NAK，等 read() 再 ACK。
pub fn on_enable<T: Instance>(index: usize, enabled: bool, dir_in: bool) {
    reset(index);
    if index == 0 {
        return;
    }
    let (n, a0) = with_ep(index, |p| (p.n, p.addrs[0]));
    if n == 0 {
        return;
    }
    set_buf_mod::<T>(index, false);
    if enabled && !dir_in {
        set_rx_dma::<T>(index, a0);
    }
}

fn arm_rx_slot<T: Instance>(index: usize, p: &mut BulkEp) -> bool {
    if p.n_hw != 0 || p.used() as usize >= p.n as usize {
        return false;
    }
    let slot = (p.cons + p.filled) % p.n;
    p.hw[0] = slot;
    p.n_hw = 1;
    set_rx_dma::<T>(index, p.addrs[slot as usize]);
    true
}

fn start_rx_locked<T: Instance>(index: usize, p: &mut BulkEp) {
    if p.n == 0 || p.dir_in {
        return;
    }
    if arm_rx_slot::<T>(index, p) {
        compiler_fence(Ordering::Release);
        T::dregs().ep_rx_ctrl(index).modify(|v| {
            v.set_mask_uep_r_res(EpRxResponse::ACK);
        });
        p.stopped = false;
    }
}

fn start_tx_locked<T: Instance>(index: usize, p: &mut BulkEp) {
    if p.n_hw != 0 || p.qlen == 0 {
        return;
    }
    let slot = p.q[p.qh as usize];
    p.qh = (p.qh + 1) % p.n;
    p.qlen -= 1;
    p.hw[0] = slot;
    p.n_hw = 1;
    set_tx_dma::<T>(index, p.addrs[slot as usize], p.lens[slot as usize]);
    compiler_fence(Ordering::Release);
    ack_tx::<T>(index);
}

/// ISR：OUT 完成。先挂下一格并清 `UIF_TRANSFER`，再返回是否需要 wake（filled 0→1）。
pub fn on_out<T: Instance>(index: usize, len: u16) -> bool {
    EVT_RX.store(EVT_RX.load(Ordering::Relaxed).wrapping_add(1), Ordering::Relaxed);
    let p = unsafe { ep_mut(index) };
    if p.n == 0 {
        clear_transfer::<T>();
        return false;
    }
    let was_empty = p.filled == 0;
    let slot = if p.hw[0] != 0xFF { p.hw[0] } else { p.cons };
    p.lens[slot as usize] = len;
    p.filled = p.filled.saturating_add(1);
    p.hw[0] = 0xFF;
    p.n_hw = 0;

    let ack = arm_rx_slot::<T>(index, p);
    compiler_fence(Ordering::Release);
    flip_rx_res::<T>(index, ack);
    p.stopped = !ack;
    clear_transfer::<T>();
    was_empty
}

/// ISR：IN 完成。先挂下一包并清标志，再返回是否需要 wake（队列从满到非满）。
pub fn on_in<T: Instance>(index: usize) -> bool {
    EVT_TX.store(EVT_TX.load(Ordering::Relaxed).wrapping_add(1), Ordering::Relaxed);
    let p = unsafe { ep_mut(index) };
    if p.n == 0 {
        clear_transfer::<T>();
        return false;
    }
    let was_full = p.used() >= p.n;
    p.hw[0] = 0xFF;
    p.n_hw = 0;
    if p.qlen > 0 {
        let slot = p.q[p.qh as usize];
        p.qh = (p.qh + 1) % p.n;
        p.qlen -= 1;
        p.hw[0] = slot;
        p.n_hw = 1;
        set_tx_dma::<T>(index, p.addrs[slot as usize], p.lens[slot as usize]);
        compiler_fence(Ordering::Release);
        flip_tx_res::<T>(index, true);
    } else {
        flip_tx_res::<T>(index, false);
    }
    clear_transfer::<T>();
    was_full
}

/// 取一包已收数据（消耗一格）。
pub fn take_rx(index: usize) -> Option<(u32, u16)> {
    if index == 0 || index >= EP_N {
        return None;
    }
    with_ep(index, |p| {
        if p.filled == 0 {
            return None;
        }
        let i = p.cons;
        p.cons = (p.cons + 1) % p.n;
        p.filled -= 1;
        Some((p.addrs[i as usize], p.lens[i as usize]))
    })
}

/// 空闲/停顿时补挂 RX（首次读、环空、刚取走一格后）。
pub fn resume_rx<T: Instance>(index: usize) {
    if index == 0 || index >= EP_N {
        return;
    }
    with_ep(index, |p| {
        if p.n == 0 {
            return;
        }
        start_rx_locked::<T>(index, p);
    });
}

/// 拷进 TX ring；成功即已入队（可能已 ACK 开传）。
pub fn try_tx_submit<T: Instance>(index: usize, data: &[u8]) -> bool {
    if index == 0 || index >= EP_N || data.len() > 512 {
        return false;
    }
    let reserved = with_ep(index, |p| {
        if p.n == 0 {
            return None;
        }
        let i = p.free_slot()?;
        p.reserve = i;
        Some((i, p.addrs[i as usize]))
    });
    let Some((i, addr)) = reserved else {
        return false;
    };
    unsafe {
        core::ptr::copy_nonoverlapping(data.as_ptr(), addr as *mut u8, data.len());
    }
    compiler_fence(Ordering::Release);

    with_ep(index, |p| {
        p.reserve = 0xFF;
        p.lens[i as usize] = data.len() as u16;
        p.q[p.qt as usize] = i;
        p.qt = (p.qt + 1) % p.n;
        p.qlen += 1;
        start_tx_locked::<T>(index, p);
    });
    true
}

/// 从 ring 槽拷到用户缓冲。
pub fn copy_from(addr: u32, dst: &mut [u8], len: usize) {
    let n = len.min(dst.len());
    compiler_fence(Ordering::Acquire);
    unsafe {
        core::ptr::copy_nonoverlapping(addr as *const u8, dst.as_mut_ptr(), n);
    }
}

/// 调试：导出端点 ring 内部状态。
pub fn dbg_state(index: usize) -> (u8, u8, u8, u8, u8) {
    if index == 0 || index >= EP_N {
        return (0, 0, 0, 0, 0);
    }
    with_ep(index, |p| (p.n, p.cons, p.filled, p.n_hw, p.qlen))
}
