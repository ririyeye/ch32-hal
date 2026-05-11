#![no_std]
#![no_main]
#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]

use hal::delay::Delay;
use hal::gpio::{Level, Output};
use hal::println;
use {ch32_hal as hal, panic_halt as _};

#[ch32_hal::entry]
fn main() -> ! {
    hal::debug::rtt_init();
    println!("Blinky starting...");
    let p = hal::init(Default::default());

    let mut led = Output::new(p.PA15, Level::Low, Default::default());
    let mut delay = Delay;

    let mut count = 0u32;
    loop {
        led.toggle();
        println!("blink: {}", count);
        count += 1;
        delay.delay_ms(1000);
    }
}
