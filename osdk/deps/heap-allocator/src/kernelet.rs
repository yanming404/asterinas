// SPDX-License-Identifier: MPL-2.0
//! Host heap is the only global allocator on the Kernelet platform.
//! This adapter supplies the per-vCPU counters used by the complete safe kernel.
use core::ops::Deref;
use ostd::{
    cpu::{CpuId, local::DynamicCpuLocal},
    prelude::Result,
};
pub struct CpuLocalBox<T>(DynamicCpuLocal<T>);
impl<T> Deref for CpuLocalBox<T> {
    type Target = DynamicCpuLocal<T>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
pub fn alloc_cpu_local<T>(init: impl FnMut(CpuId) -> T) -> Result<CpuLocalBox<T>> {
    DynamicCpuLocal::try_new(init).map(CpuLocalBox)
}
/// Platform kernel-heap usage: requested live Host heap bytes, not slab pages.
/// Host allocator overhead is excluded. No second guest slab allocator exists.
pub fn load_total_slab_size() -> usize {
    ostd::mm::heap::allocated_bytes()
}
