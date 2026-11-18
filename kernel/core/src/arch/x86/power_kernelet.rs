// SPDX-License-Identifier: MPL-2.0
//! Power requests stop only the owning sandbox; never touch host ACPI/I/O ports.
crate::register_restart_handler!(ostd::arch::power::try_restart, crate::power::Priority::HIGH);
crate::register_poweroff_handler!(
    ostd::arch::power::try_poweroff,
    crate::power::Priority::HIGH
);
pub(super) fn init() {
    assert!(
        ostd::abi::platform_initialized(),
        "Kernelet power initialized before Host Services"
    );
}
