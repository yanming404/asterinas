// SPDX-License-Identifier: MPL-2.0
//! Full Asterinas image assembler for the Kernelet platform.
#![no_std]

// Link the original #[ostd::main] entry and its complete kernel dependency graph.
extern crate asterinas as _;

/// Supplies the immutable archive consumed by the normal Asterinas init path.
#[no_mangle]
pub fn __kernelet_initramfs() -> &'static [u8] {
    include_bytes!(env!("KERNELET_INITRAMFS_PATH"))
}
