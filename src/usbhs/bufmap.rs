//! Phase 1 实验：验证 `UEP_BUF_MOD` 双缓冲的「寄存器 ↔ DATA 同步位」映射。
//!
//! 背景：`BUF_MODE` 置位后，端点的两个 DMA 寄存器变成乒乓缓冲，由 TOG 选块；但
//! 「DATA0 用本方向寄存器」这条是从官方 `CH372Device_Double_Buffering` 例程用法反推的，
//! RM 原文没在手边。Phase 2 的双缓冲 ring 直接依赖这条，所以先单独验一遍。
//!
//! 做法：完全绕开 ring——在指定端点上**预装**两块缓冲（本方向寄存器 = A、对方向 = B），
//! ISR 只做「（可选）翻 TOG + 计数 + 清标志」，**不重挂任何 DMA 指针**。主机发 N 包特征
//! 数据（第 i 包整包填 `0x10+i`，包头写 seq+!seq），随后读回 A/B 的内容与第一个包的
//! `TOG_OK`，即可判定：
//!
//! - 第一包落在哪个缓冲 + `TOG_OK` ⇒ 硬件选块是看**寄存器 TOG** 还是看**收到的 PID**；
//! - A/B 各自收到第几包 ⇒ 本方向寄存器对应 DATA0 还是 DATA1；
//! - `flip=0, auto_tog=1` 时两块是否都收到数据 ⇒ `AUTO_TOG` 到底能不能用。
//!
//! 生产固件必须 `ENABLE = false`：为 true 时 ISR 多一次原子读 + 分支，而 OUT 的窗口余量
//! 只有 ~55 ns（见 docs/CH32V307_USBHS_安全提速路径.md §4.2）。

use core::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use ch32_metapac::usbhs::vals::{EpRxResponse, EpTog};

use super::Instance;

/// 探针开关。跑 Phase 1 时置 true，跑完（或生产）置回 false——置 false 时下面的分支
/// 会被编译器整段删掉，热路径零成本。
pub const ENABLE: bool = false;

const BUF_LEN: usize = 512;
/// 缓冲铺底值：用来区分「没被写过」和「收到过数据」。
const FILL_A: u8 = 0xA5;
const FILL_B: u8 = 0x5A;

#[repr(C, align(4))]
struct MapBuf {
    data: [u8; BUF_LEN],
}

static mut BUF_A: MapBuf = MapBuf { data: [0; BUF_LEN] };
static mut BUF_B: MapBuf = MapBuf { data: [0; BUF_LEN] };

static ACTIVE: AtomicBool = AtomicBool::new(false);
static EP: AtomicU32 = AtomicU32::new(0);
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

/// 启动探针：`ep` 是端点号（OUT 方向），`flip` = ISR 是否软件翻 TOG，
/// `auto_tog` = 是否打开硬件自动翻（`UEP_R_AUTO_TOG`）。
///
/// 返回 false 表示参数非法或探针被 `ENABLE` 关掉。
pub fn start<T: Instance>(ep: usize, flip: bool, auto_tog: bool) -> bool {
    if !ENABLE || ep == 0 || ep >= 16 {
        return false;
    }
    // 先清掉上一次的状态（不唤醒 ring，start 会立刻重新配置硬件）。
    ACTIVE.store(false, Ordering::Relaxed);

    unsafe {
        core::ptr::write_bytes(core::ptr::addr_of_mut!(BUF_A.data) as *mut u8, FILL_A, BUF_LEN);
        core::ptr::write_bytes(core::ptr::addr_of_mut!(BUF_B.data) as *mut u8, FILL_B, BUF_LEN);
    }

    let d = T::dregs();
    // OUT 端点：本方向 RX_DMA = A，对方向 TX_DMA = B（待验证的假设）
    d.ep_rx_dma(ep - 1).write_value(buf_addr_a());
    d.ep_tx_dma(ep - 1).write_value(buf_addr_b());
    d.ep_max_len(ep).write(|w| w.set_len(BUF_LEN as u16));
    d.ep_buf_mod().modify(|w| w.set_buf_mod(ep, true));
    d.ep_rx_ctrl(ep).write(|w| {
        w.set_mask_uep_r_tog(EpTog::DATA0); // 从 DATA0 起，和官方用法一致
        w.set_mask_uep_r_res(EpRxResponse::ACK);
        w.set_r_tog_auto(auto_tog);
    });

    EP.store(ep as u32, Ordering::Relaxed);
    FLIP.store(flip, Ordering::Relaxed);
    AUTO_TOG.store(auto_tog, Ordering::Relaxed);
    COUNT.store(0, Ordering::Relaxed);
    FIRST_TOG_OK.store(0xFFFF_FFFF, Ordering::Relaxed);
    LAST_TOG_OK.store(0xFFFF_FFFF, Ordering::Relaxed);
    LAST_LEN.store(0, Ordering::Relaxed);
    ACTIVE.store(true, Ordering::Relaxed);
    true
}

/// ISR 钩子：探针激活时替代 ring 处理 OUT 完成。**不碰 DMA 指针**，只计数、
/// （可选）翻 TOG、清 `UIF_TRANSFER`。
#[inline(always)]
pub fn on_out<T: Instance>(ep: usize, tog_ok: bool, len: u16) {
    let n = COUNT.load(Ordering::Relaxed).wrapping_add(1);
    COUNT.store(n, Ordering::Relaxed);
    if n == 1 {
        FIRST_TOG_OK.store(tog_ok as u32, Ordering::Relaxed);
    }
    LAST_TOG_OK.store(tog_ok as u32, Ordering::Relaxed);
    LAST_LEN.store(len as u32, Ordering::Relaxed);

    if FLIP.load(Ordering::Relaxed) {
        // 软件推进乒乓：翻 R_TOG、保持 ACK（等价官方例程里那句 `UEPn_RX_CTRL ^= TOG`）
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
    // 清标志，结束 INT_BUSY 窗口
    T::regs().int_fg().write(|v| v.set_transfer(true));
}

/// 停止探针并把端点还给 ring：关 `BUF_MODE`、改回 NAK，复位该端点的 ring 状态并唤醒，
/// 应用侧的 `recv()/release()` 会重新 `kick_rx`。
pub fn stop<T: Instance>() {
    let ep = EP.load(Ordering::Relaxed) as usize;
    ACTIVE.store(false, Ordering::Relaxed);
    if ep == 0 || ep >= 16 {
        return;
    }
    let d = T::dregs();
    d.ep_buf_mod().modify(|w| w.set_buf_mod(ep, false));
    d.ep_rx_ctrl(ep).write(|w| {
        w.set_mask_uep_r_res(EpRxResponse::NAK);
        w.set_r_tog_auto(false);
    });
    super::pipe::reset(ep);
    super::EP_WAKERS[ep].wake();
}

/// 报告布局（16 × u32 LE，主机侧按同序解析）：
///
/// | idx | 含义 |
/// |---|---|
/// | 0 | 收到的包数 |
/// | 1 | 第一包的 `TOG_OK`（`0xFFFF_FFFF` = 还没收到） |
/// | 2 | 最后一包的 `TOG_OK` |
/// | 3 | `UEPn_RX_CTRL` 原始值 |
/// | 4 | 最后一包的 `RX_LEN` |
/// | 5 | `BUF_A[0..4]`（seq）/ 6 `BUF_A[4..8]`（!seq）/ 7 `BUF_A[8]`（特征字节）/ 8 `BUF_A[16..20]` |
/// | 9..12 | 同上的 `BUF_B` |
/// | 13 | 端点号 / 14 `flip as u32 \| auto_tog<<1` / 15 `BUF_MODE` 寄存器 |
pub fn report<T: Instance>() -> [u32; 16] {
    let ep = EP.load(Ordering::Relaxed) as usize;
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
    out[14] = FLIP.load(Ordering::Relaxed) as u32 | ((AUTO_TOG.load(Ordering::Relaxed) as u32) << 1);
    if ep != 0 && ep < 16 {
        // metapac 的 fieldset 没有 to_bits()，这里按位重建一个够用的字节：
        // bit0-1 = R_RES，bit2-3 = R_TOG，bit4 = R_TOG_AUTO
        let d = T::dregs();
        let c = d.ep_rx_ctrl(ep).read();
        out[3] = c.mask_uep_r_res().to_bits() as u32
            | ((c.mask_uep_r_tog().to_bits() as u32) << 2)
            | ((c.r_tog_auto() as u32) << 4);
        out[15] = d.ep_buf_mod().read().buf_mod(ep) as u32;
    }
    out
}
