// SPDX-License-Identifier: MPL-2.0
//! Socket-facing transport selection. Both implementations feed the existing
//! connecting/listening/connected stream states and their signal-aware waits.
use super::{addr::VsockSocketAddr, transport as virtio};
use crate::{
    events::IoEvents,
    net::socket::util::{RecvFlags, SendFlags, SockShutdownCmd},
    prelude::*,
    process::signal::Pollee,
    util::{MultiRead, MultiWrite},
};
#[cfg(feature = "typedio")]
#[path = "typedio.rs"]
mod typedio;

pub(super) enum BoundPort {
    Virtio(virtio::BoundPort),
    #[cfg(feature = "typedio")]
    Typedio(typedio::BoundPort),
}
pub(super) enum Connection {
    Virtio(virtio::Connection),
    #[cfg(feature = "typedio")]
    Typedio(typedio::Connection),
}
pub(super) enum Listener {
    Virtio(virtio::Listener),
    #[cfg(feature = "typedio")]
    Typedio(typedio::Listener),
}
pub(super) enum ConnectResult {
    Connecting(Connection),
    Connected(Connection),
    Failed(BoundPort, Error),
}
impl BoundPort {
    pub(super) fn new_exclusive(address: VsockSocketAddr) -> Result<Self> {
        #[cfg(feature = "typedio")]
        if typedio::owns(address.cid) {
            return typedio::BoundPort::new_exclusive(address).map(Self::Typedio);
        }
        virtio::BoundPort::new_exclusive(address).map(Self::Virtio)
    }
    pub(super) fn new_ephemeral() -> Result<Self> {
        #[cfg(feature = "typedio")]
        if typedio::owns(super::addr::VMADDR_CID_ANY) {
            return typedio::BoundPort::new_ephemeral().map(Self::Typedio);
        }
        virtio::BoundPort::new_ephemeral().map(Self::Virtio)
    }
    pub(super) fn connect(
        self,
        address: VsockSocketAddr,
        pollee: &Pollee,
    ) -> Result<Connection, (Error, Self)> {
        match self {
            Self::Virtio(port) => port
                .connect(address, pollee)
                .map(Connection::Virtio)
                .map_err(|(e, p)| (e, Self::Virtio(p))),
            #[cfg(feature = "typedio")]
            Self::Typedio(port) => port
                .connect(address, pollee)
                .map(Connection::Typedio)
                .map_err(|(e, p)| (e, Self::Typedio(p))),
        }
    }
    pub(super) fn listen(self, backlog: usize, pollee: &Pollee) -> Result<Listener, (Error, Self)> {
        match self {
            Self::Virtio(port) => port
                .listen(backlog, pollee)
                .map(Listener::Virtio)
                .map_err(|(e, p)| (e, Self::Virtio(p))),
            #[cfg(feature = "typedio")]
            Self::Typedio(port) => port
                .listen(backlog, pollee)
                .map(Listener::Typedio)
                .map_err(|(e, p)| (e, Self::Typedio(p))),
        }
    }
    pub(super) fn local_addr(&self) -> VsockSocketAddr {
        match self {
            Self::Virtio(p) => p.local_addr(),
            #[cfg(feature = "typedio")]
            Self::Typedio(p) => p.local_addr(),
        }
    }
    pub(super) fn permits_peer(&self, cid: u32) -> bool {
        match self {
            Self::Virtio(_) => cid == super::addr::VMADDR_CID_HOST,
            #[cfg(feature = "typedio")]
            Self::Typedio(p) => p.permits_peer(cid),
        }
    }
}
macro_rules! connection_delegate {($name:ident($($arg:ident:$ty:ty),*)->$result:ty)=>{pub(super) fn $name(&self,$($arg:$ty),*)->$result {match self{Self::Virtio(c)=>c.$name($($arg),*),#[cfg(feature="typedio")]Self::Typedio(c)=>c.$name($($arg),*)}}};}
impl Connection {
    connection_delegate!(local_addr()->VsockSocketAddr);
    connection_delegate!(remote_addr()->VsockSocketAddr);
    connection_delegate!(pollee()->&Pollee);
    connection_delegate!(check_io_events()->IoEvents);
    connection_delegate!(test_and_clear_error()->Result<()>);
    connection_delegate!(has_connect_result()->bool);
    connection_delegate!(shutdown(command:SockShutdownCmd)->Result<()>);
    pub(super) fn try_send(
        &mut self,
        reader: &mut dyn MultiRead,
        flags: SendFlags,
    ) -> Result<usize> {
        match self {
            Self::Virtio(c) => c.try_send(reader, flags),
            #[cfg(feature = "typedio")]
            Self::Typedio(c) => c.try_send(reader, flags),
        }
    }
    pub(super) fn try_recv(
        &mut self,
        writer: &mut dyn MultiWrite,
        flags: RecvFlags,
    ) -> Result<usize> {
        match self {
            Self::Virtio(c) => c.try_recv(writer, flags),
            #[cfg(feature = "typedio")]
            Self::Typedio(c) => c.try_recv(writer, flags),
        }
    }
    pub(super) fn finish_connect(self) -> ConnectResult {
        match self {
            Self::Virtio(c) => match c.finish_connect() {
                virtio::ConnectResult::Connecting(c) => ConnectResult::Connecting(Self::Virtio(c)),
                virtio::ConnectResult::Connected(c) => ConnectResult::Connected(Self::Virtio(c)),
                virtio::ConnectResult::Failed(p, e) => {
                    ConnectResult::Failed(BoundPort::Virtio(p), e)
                }
            },
            #[cfg(feature = "typedio")]
            Self::Typedio(c) => match c.finish_connect() {
                typedio::ConnectResult::Connecting(c) => {
                    ConnectResult::Connecting(Self::Typedio(c))
                }
                typedio::ConnectResult::Connected(c) => ConnectResult::Connected(Self::Typedio(c)),
                typedio::ConnectResult::Failed(p, e) => {
                    ConnectResult::Failed(BoundPort::Typedio(p), e)
                }
            },
        }
    }
}
impl Listener {
    pub(super) fn try_accept(&self) -> Result<Connection> {
        match self {
            Self::Virtio(l) => l.try_accept().map(Connection::Virtio),
            #[cfg(feature = "typedio")]
            Self::Typedio(l) => l.try_accept().map(Connection::Typedio),
        }
    }
    pub(super) fn set_backlog(&self, n: usize) {
        match self {
            Self::Virtio(l) => l.set_backlog(n),
            #[cfg(feature = "typedio")]
            Self::Typedio(l) => l.set_backlog(n),
        }
    }
    pub(super) fn local_addr(&self) -> VsockSocketAddr {
        match self {
            Self::Virtio(l) => l.local_addr(),
            #[cfg(feature = "typedio")]
            Self::Typedio(l) => l.local_addr(),
        }
    }
    pub(super) fn check_io_events(&self) -> IoEvents {
        match self {
            Self::Virtio(l) => l.check_io_events(),
            #[cfg(feature = "typedio")]
            Self::Typedio(l) => l.check_io_events(),
        }
    }
}
pub(super) fn init() {
    virtio::init();
    #[cfg(feature = "typedio")]
    typedio::init();
}
