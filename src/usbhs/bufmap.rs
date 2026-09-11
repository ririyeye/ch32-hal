//! Phase 1/3 实验：验证 `UEP_BUF_MOD` 双缓冲的「寄存器 ↔ DATA 同步位」映射，
//! 以及 `AUTO_TOG` 到底能不能用（RX / TX 分开验）。
//!
//! 背景：`BUF_MODE` 置位后，端点的两个 DMA 寄存器变成乒乓缓冲，由 TOG 选块；但
//! 「DATA0 用本方向寄存器」这条是从官方 `CH372Device_Double_Buffering` 例程用法反推的，
//! RM 原文没在手边。Phase 2/3 的双缓冲 ring 直接依赖这条，所以先单独验一遍。
//!
//! 做法：完全绕开 ring——在指定端点上**预装**两块缓冲（本方向寄存器 = A、对方向 = B），
//! ISR 只做「（可选）翻 TOG + 计数 + 清标志」，**不重挂任何 DMA 指针**。
//!
//! - RX（OUT）探针：主机发 N 包特征数据，随后读回 A/B 内容与首包 `TOG_OK`；
//! - TX（IN）探针：主机连读 N 包，看拿到的是 A 的特征字节还是 B 的（乒乓是否推进）。
//!
//! 生产固件必须 `ENABLE = false`：为 true 时 ISR 多一次原子读 + 分支，而 OUT 的窗口余量
//! 只有 ~55 ns（见 docs/CH32V307_USBHS_安全提速路径.md §4.2）。

use core::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use ch32_metapac::usbhs::vals::{EpRxResponse, EpTog, EpTxResponse};

use super::Instance;

/// 探针开关。跑实验时置 true，跑完（或生产）置回 false——置 false 时 ISR 里的分支
/// 会被编译器整段删掉，热路径零成本。
pub const ENABLE: bool = false;

const BUF_LEN: usize = 512;
/// RX 探针铺底值：用来区分「没被写过」和「收到过数据」。
const FILL_RX_A: u8 = 0xA5;
const FILL_RX_B: u8 = 0x5A;
/// TX 探针特征值：主机读回来据此判断这一包是从哪块缓冲发出去的。
const FILL_TX_A: u8 = 0xA1;
const FILL_TX_B: u8 = 0xB1;

#[repr(C, align(4))]
struct MapBuf {
    data: [u8; BUF_LEN],
}

static mut BUF_A: MapBuf = MapBuf { data: [0; BUF_LEN] };
static mut BUF_B: MapBuf = MapBuf { data: [0; BUF_LEN] };

static ACTIVE: AtomicBool = AtomicBool::new(false);
static EP: AtomicU32 = AtomicU32::new(0);
static IS_TX: AtomicBool = AtomicBool::new(false);
static FLIP: AtomicBool = AtomicBool::new(false);
static AUTO_TOG: AtomicBool = AtomicBool::new(false);
static COUNT: AtomicU32 = AtomicU32::new(0);
static FIRST_TOG_OK: AtomicU32 = AtomicU32::new(0xFFFF_FFFF);
static LAST_TOG_OK: AtomicU32 = AtomicU32::new(0xFFFF_FFFF);
static LAST_LEN: AtomicU32 = AtomicU32::new(0);

/// 探针是否正作用在该端点上（ISR 热路径只读原子位）。
#[inline(always)]
pub fn active(ep: usize) -> bool {
    ACTIVE.load(Ordering::Relaxed) && EP.load(Ordering::Relaxed) as usize == ep
}

#[inline(always)]
fn buf_addr_a() -> u32 {
    unsafe { core::ptr::addr_of!(BUF_A.data) as *const u8 as u32 }
}

#[inline(always)]
fn buf_addr_b() -> u32 {
    unsafe { core::ptr::addr_of!(BUF_B.data) as *const u8 as u32 }
}

#[inline(always)]
fn rd_u32(addr: u32, off: usize) -> u32 {
    unsafe { core::ptr::read_volatile((addr as *const u8).add(off) as *const u32) }
}

#[inline(always)]
fn rd_u8(addr: u32, off: usize) -> u8 {
    unsafe { core::ptr::read_volatile((addr as *const u8).add(off)) }
}

fn fill(addr: u32, v: u8) {
    unsafe { core::ptr::write_bytes(addr as *mut u8, v, BUF_LEN) }
}

/// 启动探针。`tx=true` 时作用于 IN 端点（TX 方向），否则作用于 OUT 端点（RX 方向）。
///
/// `flip` = ISR 是否软件翻 TOG；`auto_tog` = 是否打开 `UEP_{R,T}_AUTO_TOG`。
/// 返回 false 表示参数非法或探针被 `ENABLE` 关掉。
pub fn start<T: Instance>(ep: usize, tx: bool, flip: bool, auto_tog: bool) -> bool {
    if !ENABLE || ep == 0 || ep >= 16 {
        return false;
    }
    ACTIVE.store(false, Ordering::Relaxed);

    let d = T::dregs();
    d.ep_max_len(ep).write(|w| w.set_len(BUF_LEN as u16));
    d.ep_buf_mod().modify(|w| w.set_buf_mod(ep, true));

    if tx {
        // IN 端点：本方向 TX_DMA = A（DATA0），对方向 RX_DMA = B（DATA1）
        fill(buf_addr_a(), FILL_TX_A);
        fill(buf_addr_b(), FILL_TX_B);
        d.ep_tx_dma(ep - 1).write_value(buf_addr_a());
        d.ep_rx_dma(ep - 1).write_value(buf_addr_b());
        d.ep_t_len(ep).write(|w| w.set_len(BUF_LEN as u16));
        d.ep_tx_ctrl(ep).write(|w| {
            w.set_mask_uep_t_tog(EpTog::DATA0);
            w.set_mask_uep_t_res(EpTxResponse::ACK);
            w.set_t_tog_auto(auto_tog);
        });
    } else {
        // OUT 端点：本方向 RX_DMA = A（DATA0），对方向 TX_DMA = B（DATA1）
        fill(buf_addr_a(), FILL_RX_A);
        fill(buf_addr_b(), FILL_RX_B);
        d.ep_rx_dma(ep - 1).write_value(buf_addr_a());
        d.ep_tx_dma(ep - 1).write_value(buf_addr_b());
        d.ep_rx_ctrl(ep).write(|w| {
            w.set_mask_uep_r_tog(EpTog::DATA0);
            w.set_mask_uep_r_res(EpRxResponse::ACK);
            w.set_r_tog_auto(auto_tog);
        });
    }

    EP.store(ep as u32, Ordering::Relaxed);
    IS_TX.store(tx, Ordering::Relaxed);
    FLIP.store(flip, Ordering::Relaxed);
    AUTO_TOG.store(auto_tog, Ordering::Relaxed);
    COUNT.store(0, Ordering::Relaxed);
    FIRST_TOG_OK.store(0xFFFF_FFFF, Ordering::Relaxed);
    LAST_TOG_OK.store(0xFFFF_FFFF, Ordering::Relaxed);
    LAST_LEN.store(0, Ordering::Relaxed);
    ACTIVE.store(true, Ordering::Relaxed);
    true
}

/// 记录一次完成事件（两个方向共用）。
#[inline(always)]
fn note(tog_ok: bool, len: u16) {
    let n = COUNT.load(Ordering::Relaxed).wrapping_add(1);
    COUNT.store(n, Ordering::Relaxed);
    if n == 1 {
        FIRST_TOG_OK.store(tog_ok as u32, Ordering::Relaxed);
    }
    LAST_TOG_OK.store(tog_ok as u32, Ordering::Relaxed);
    LAST_LEN.store(len as u32, Ordering::Relaxed);
}

/// ISR 钩子：探针激活时替代 ring 处理 OUT 完成。**不碰 DMA 指针**。
#[inline(always)]
pub fn on_out<T: Instance>(ep: usize, tog_ok: bool, len: u16) {
    note(tog_ok, len);
    if FLIP.load(Ordering::Relaxed) {
        // 软件推进乒乓（等价官方例程那句 `UEPn_RX_CTRL ^= TOG`）；注意这会关掉 AUTO_TOG
        let c = T::dregs().ep_rx_ctrl(ep).read();
        let next = if let EpTog::DATA0 = c.mask_uep_r_tog() {
            EpTog::DATA1
        } else {
            EpTog::DATA0
        };
        T::dregs().ep_rx_ctrl(ep).write(|w| {
            w.set_mask_uep_r_tog(next);
            w.set_mask_uep_r_res(EpRxResponse::ACK);
            w.set_r_tog_auto(false);
        });
    }
    T::regs().int_fg().write(|v| v.set_transfer(true));
}

/// ISR 钩子：TX 方向。探针激活时替代 ring 处理 IN 完成。**不碰 DMA 指针**。
#[inline(always)]
pub fn on_in<T: Instance>(ep: usize, tog_ok: bool) {
    note(tog_ok, BUF_LEN as u16);
    if FLIP.load(Ordering::Relaxed) {
        let c = T::dregs().ep_tx_ctrl(ep).read();
        let next = if let EpTog::DATA0 = c.mask_uep_t_tog() {
            EpTog::DATA1
        } else {
            EpTog::DATA0
        };
        T::dregs().ep_tx_ctrl(ep).write(|w| {
            w.set_mask_uep_t_tog(next);
            w.set_mask_uep_t_res(EpTxResponse::ACK);
            w.set_t_tog_auto(false);
        });
    }
    T::regs().int_fg().write(|v| v.set_transfer(true));
}

/// 停止探针并把端点还给 ring：关 `BUF_MODE`、改回 NAK，复位该端点的 ring 状态并唤醒，
/// 应用侧的 `recv()/release()`（RX）或 `alloc()/submit()`（TX）会重新 kick。
pub fn stop<T: Instance>() {
    let ep = EP.load(Ordering::Relaxed) as usize;
    let tx = IS_TX.load(Ordering::Relaxed);
    ACTIVE.store(false, Ordering::Relaxed);
    if ep == 0 || ep >= 16 {
        return;
    }
    let d = T::dregs();
    d.ep_buf_mod().modify(|w| w.set_buf_mod(ep, false));
    if tx {
        d.ep_tx_ctrl(ep).write(|w| {
            w.set_mask_uep_t_res(EpTxResponse::NAK);
            w.set_t_tog_auto(false);
        });
    } else {
        d.ep_rx_ctrl(ep).write(|w| {
            w.set_mask_uep_r_res(EpRxResponse::NAK);
            w.set_r_tog_auto(false);
        });
    }
    super::pipe::reset(ep);
    super::EP_WAKERS[ep].wake();
}

/// 报告布局（16 × u32 LE，主机侧按同序解析）：
///
/// | idx | 含义 |
/// |---|---|
/// | 0 | 完成的包数 |
/// | 1 | 第一包的 `TOG_OK`（`0xFFFF_FFFF` = 还没发生） |
/// | 2 | 最后一包的 `TOG_OK` |
/// | 3 | 本方向 CTRL 重建值：bit0-1=RES、bit2-3=TOG、bit4=AUTO |
/// | 4 | 最后一包的 `RX_LEN`（TX 探针恒为 512） |
/// | 5..8 | `BUF_A`：`[0..4]`、`[4..8]`、`[8]`、`[16..20]` |
/// | 9..12 | `BUF_B`：同上 |
/// | 13 | 端点号 |
/// | 14 | `flip \| auto_tog<<1 \| tx<<2` |
/// | 15 | `BUF_MODE` 里该端点的位 |
pub fn report<T: Instance>() -> [u32; 16] {
    let ep = EP.load(Ordering::Relaxed) as usize;
    let tx = IS_TX.load(Ordering::Relaxed);
    let (a, b) = (buf_addr_a(), buf_addr_b());
    let mut out = [0u32; 16];
    out[0] = COUNT.load(Ordering::Relaxed);
    out[1] = FIRST_TOG_OK.load(Ordering::Relaxed);
    out[2] = LAST_TOG_OK.load(Ordering::Relaxed);
    out[4] = LAST_LEN.load(Ordering::Relaxed);
    out[5] = rd_u32(a, 0);
    out[6] = rd_u32(a, 4);
    out[7] = rd_u8(a, 8) as u32;
    out[8] = rd_u32(a, 16);
    out[9] = rd_u32(b, 0);
    out[10] = rd_u32(b, 4);
    out[11] = rd_u8(b, 8) as u32;
    out[12] = rd_u32(b, 16);
    out[13] = ep as u32;
    out[14] = FLIP.load(Ordering::Relaxed) as u32
        | ((AUTO_TOG.load(Ordering::Relaxed) as u32) << 1)
        | ((tx as u32) << 2);
    if ep != 0 && ep < 16 {
        let d = T::dregs();
        let c = if tx {
            let c = d.ep_tx_ctrl(ep).read();
            c.mask_uep_t_res().to_bits() as u32
                | ((c.mask_uep_t_tog().to_bits() as u32) << 2)
                | ((c.t_tog_auto() as u32) << 4)
        } else {
            let c = d.ep_rx_ctrl(ep).read();
            c.mask_uep_r_res().to_bits() as u32
                | ((c.mask_uep_r_tog().to_bits() as u32) << 2)
                | ((c.r_tog_auto() as u32) << 4)
        };
        out[3] = c;
        out[15] = d.ep_buf_mod().read().buf_mod(ep) as u32;
    }
    out
}
