// SPDX-License-Identifier: MPL-2.0

pub(crate) mod cpu;
#[cfg_attr(feature = "kernelet", path = "power_kernelet.rs")]
mod power;
pub(crate) mod ptrace;
pub(crate) mod signal;

pub(crate) fn init() {
    power::init();
}
