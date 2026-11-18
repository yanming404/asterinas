// SPDX-License-Identifier: MPL-2.0
//! A Kernelet has no hardware VirtIO Vsock device. Preserve the Socket-facing
//! states while selecting the real TypedIO implementation exclusively.
#[path = "typedio.rs"]
mod typedio;
pub(super) use typedio::{BoundPort, ConnectResult, Connection, Listener, init};
