// SPDX-License-Identifier: MPL-2.0
//! Shared TypedIO endpoints and the trusted platform installation boundary.
use ::typedio::{notification::Binding, service::Call};

use crate::irq::IrqLine;
#[path = "../../../kernelet/prototype/libs/typedio/src/frontend.rs"]
mod common;
pub use common::*;
type HostService = unsafe extern "C" fn(*mut Call) -> i64;
type IrqRoute = fn(&IrqLine) -> Result<Binding, BindError>;
struct Hooks {
    service: HostService,
    irq_route: Option<IrqRoute>,
}
static HOOKS: spin::Once<Hooks> = spin::Once::new();
/// Installs the TypedIO transport supplied by a trusted Kernelet boot adapter.
/// Hardware OSTD leaves this absent and discovers no TypedIO attachments.
///
/// # Safety
/// Call once in bootstrap, before other CPUs or TypedIO callers start. Verify
/// image/service ABI versions first. The entry must authenticate the caller,
/// retain all imported backing and return only reservation memory mapped into
/// this kernel's untyped domain. IRQ routes must identify this line's fixed
/// vCPU/non-reused generation and connect its local dispatch to virtual upcalls.
/// None selects the explicitly enabled unbound polling transport policy.
pub unsafe fn install_host_service(service: HostService, irq_route: Option<IrqRoute>) {
    HOOKS.call_once(|| Hooks { service, irq_route });
}
fn invoke(call: &mut Call) -> i64 {
    match HOOKS.get() {
        Some(h) => unsafe { (h.service)(call) },
        None => -19,
    }
}
fn irq_route(line: &IrqLine) -> Result<Binding, BindError> {
    let route = HOOKS
        .get()
        .and_then(|hooks| hooks.irq_route)
        .ok_or(BindError(-19))?;
    route(line)
}
