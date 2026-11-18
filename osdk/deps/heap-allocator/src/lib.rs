// SPDX-License-Identifier: MPL-2.0

#![feature(allocator_api)]
#![no_std]
#![deny(unsafe_code)]

extern crate alloc;

#[cfg(not(feature = "kernelet"))]
use core::sync::atomic::AtomicIsize;

#[cfg(not(feature = "kernelet"))]
use ostd::cpu_local;

// Set this crate's log prefix for `ostd::log`.
#[cfg(not(feature = "kernelet"))]
macro_rules! __log_prefix {
    () => {
        "heap: "
    };
}

#[cfg(not(feature = "kernelet"))]
mod allocator;
#[cfg(not(feature = "kernelet"))]
mod cpu_local_allocator;
#[cfg(not(feature = "kernelet"))]
mod slab_cache;
#[cfg(not(feature = "kernelet"))]
mod slab_counter;
#[cfg(ktest)]
#[cfg(not(feature = "kernelet"))]
mod test;

#[cfg(not(feature = "kernelet"))]
pub use allocator::{HeapAllocator, type_from_layout};
#[cfg(not(feature = "kernelet"))]
pub use cpu_local_allocator::{CpuLocalBox, alloc_cpu_local};

#[cfg(not(feature = "kernelet"))]
cpu_local! {
    static LOCAL_TOTAL_SLAB_ALLOCATED: AtomicIsize = AtomicIsize::new(0);
}

/// Total size (in bytes) of physical memory committed to the slab caches.
#[cfg(not(feature = "kernelet"))]
pub(crate) static TOTAL_SLAB_ALLOCATED: slab_counter::SlabCounter =
    slab_counter::SlabCounter::new(&LOCAL_TOTAL_SLAB_ALLOCATED);

/// Returns the total size (in bytes) of memory committed to the slab caches.
#[cfg(not(feature = "kernelet"))]
pub fn load_total_slab_size() -> usize {
    TOTAL_SLAB_ALLOCATED.get()
}

#[cfg(feature = "kernelet")]
mod kernelet;
#[cfg(feature = "kernelet")]
pub use kernelet::{CpuLocalBox, alloc_cpu_local, load_total_slab_size};
