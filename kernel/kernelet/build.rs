// SPDX-License-Identifier: MPL-2.0

use std::{env, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-env-changed=KLET_INITRAMFS");
    let archive = PathBuf::from(
        env::var_os("KLET_INITRAMFS")
            .expect("set KLET_INITRAMFS to a real CPIO archive; see kernel/kernelet/README.md"),
    )
    .canonicalize()
    .expect("KLET_INITRAMFS must be readable");
    let size = std::fs::metadata(&archive).unwrap().len();
    assert!(size >= 110, "initramfs is too small for a newc archive");
    println!("cargo:rerun-if-changed={}", archive.display());
    println!(
        "cargo:rustc-env=KERNELET_INITRAMFS_PATH={}",
        archive.display()
    );
}
