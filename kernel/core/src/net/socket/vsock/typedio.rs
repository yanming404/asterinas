// SPDX-License-Identifier: MPL-2.0
//! TypedIO driver feeding the existing Vsock socket stream states. The
//! default path uses interest-gated IRQs; unbound polling is an explicit feature.
//! No socket-state lock is held while copying application memory.
use core::{
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    time::Duration,
};

use aster_softirq::{BottomHalfDisabled, Taskless};
use ostd::{
    irq::IrqLine,
    typedio::{
        self, Consumer, ConsumerEvents, ConsumerInterest, PipeError, Producer, ProducerEvents,
        ProducerInterest, Queue, QueueEvents, QueueOptions, RequestTag, vsock::*,
    },
};
use spin::Once;

use super::super::addr::{VMADDR_CID_ANY, VMADDR_PORT_ANY, VsockSocketAddr};
use crate::{
    events::IoEvents,
    net::socket::util::{RecvFlags, SendFlags, SockShutdownCmd},
    prelude::*,
    process::signal::Pollee,
    time::{
        Timer,
        clocks::JIFFIES_TIMER_MANAGER,
        timer::{Timeout, TimerGuard},
    },
    util::{MultiRead, MultiWrite},
};

type Key = (u32, u64, u32, u64);
const CONTROL_LIMIT: usize = 64;
#[cfg(feature = "typedio-polling")]
const POLL_PERIOD: Duration = Duration::from_millis(1);
static DEVICES: Once<Vec<Arc<Space>>> = Once::new();
struct Registry {
    ports: BTreeMap<u32, usize>,
    next_port: u32,
    connections: BTreeMap<Key, Weak<Conn>>,
    listeners: BTreeMap<u32, Weak<ListenerInner>>,
    controls: BTreeMap<u64, VsockControlMessage>,
    pending: VecDeque<(u64, VsockControlMessage)>,
}
struct Space {
    cid: u32,
    config: VsockConfig,
    control: Queue<VsockProtocol>,
    receive: Queue<VsockProtocol>,
    registry: SpinLock<Registry, BottomHalfDisabled>,
    next_tag: AtomicU64,
    next_attempt: AtomicU64,
    posted: AtomicUsize,
    removed: AtomicBool,
    busy: AtomicBool,
    work: Once<Arc<Taskless>>,
    irq: Once<IrqLine>,
    #[cfg(feature = "typedio-polling")]
    timer: Once<Arc<Timer>>,
}
struct PortLease {
    space: Arc<Space>,
    port: u32,
}
#[derive(Clone)]
pub(in crate::net::socket::vsock) struct BoundPort {
    lease: Arc<PortLease>,
}
impl Debug for BoundPort {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TypedIoPort")
            .field("cid", &self.lease.space.cid)
            .field("port", &self.lease.port)
            .finish()
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Connecting,
    Connected,
    Failed,
    Closed,
}
struct ConnState {
    phase: Phase,
    error: Option<Error>,
    tx: Option<Producer>,
    rx: Option<Consumer>,
    tx_ready: bool,
    rx_ready: bool,
    local_read: bool,
    local_write: bool,
    peer_read: bool,
    peer_write: bool,
    timer: Option<Arc<Timer>>,
}
struct Conn {
    timeout: AtomicBool,
    bound: BoundPort,
    key: Key,
    pollee: Pollee,
    state: SpinLock<ConnState, BottomHalfDisabled>,
    listener: Option<Weak<ListenerInner>>,
}
pub(in crate::net::socket::vsock) struct Connection {
    inner: Option<Arc<Conn>>,
}
pub(in crate::net::socket::vsock) enum ConnectResult {
    Connecting(Connection),
    Connected(Connection),
    Failed(BoundPort, Error),
}
struct Pending {
    backlog: usize,
    connections: VecDeque<Arc<Conn>>,
}
struct ListenerInner {
    bound: BoundPort,
    pollee: Pollee,
    pending: SpinLock<Pending, BottomHalfDisabled>,
}
pub(in crate::net::socket::vsock) struct Listener {
    inner: Arc<ListenerInner>,
}

pub(in crate::net::socket::vsock) fn owns(cid: u32) -> bool {
    DEVICES.get().is_some_and(|spaces| {
        spaces
            .iter()
            .any(|s| !s.removed.load(Ordering::Acquire) && (cid == VMADDR_CID_ANY || cid == s.cid))
    })
}
fn select(cid: u32) -> Result<Arc<Space>> {
    DEVICES
        .get()
        .and_then(|s| {
            s.iter().find(|s| {
                !s.removed.load(Ordering::Acquire) && (cid == VMADDR_CID_ANY || cid == s.cid)
            })
        })
        .cloned()
        .ok_or_else(|| {
            Error::with_message(Errno::EADDRNOTAVAIL, "no TypedIO attachment owns this CID")
        })
}
fn identity(counter: &AtomicU64) -> Result<u64> {
    counter
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_add(1))
        .map_err(|_| Error::with_message(Errno::EOVERFLOW, "TypedIO identities exhausted"))
}
fn address(a: VsockSocketAddr) -> Address {
    Address {
        cid: ContextId(a.cid as u64),
        port: Port(a.port),
    }
}
fn key(flow: Flow, attempt: AttemptId) -> Key {
    (
        flow.destination.port.0,
        flow.source.cid.0,
        flow.source.port.0,
        attempt.0,
    )
}
fn io_error(error: PipeError) -> Error {
    Error::with_message(
        match error {
            PipeError::WouldBlock | PipeError::Busy => Errno::EAGAIN,
            PipeError::Closed => Errno::EPIPE,
            PipeError::Reset => Errno::ECONNRESET,
            _ => Errno::EIO,
        },
        "TypedIO pipe operation failed",
    )
}
fn partial(result: Result<usize, (ostd::Error, usize)>) -> Result<usize> {
    match result {
        Ok(n) => Ok(n),
        Err((_, n)) if n != 0 => Ok(n),
        Err((error, _)) => Err(error.into()),
    }
}

impl BoundPort {
    pub(in crate::net::socket::vsock) fn new_exclusive(addr: VsockSocketAddr) -> Result<Self> {
        let space = select(addr.cid)?;
        let mut registry = space.registry.lock();
        if registry.ports.len() >= 128 {
            return_errno_with_message!(Errno::ENOBUFS, "TypedIO bound-port quota exhausted")
        }
        let port = if addr.port == VMADDR_PORT_ANY {
            let mut port = registry.next_port;
            let mut found = None;
            for _ in 0..129 {
                if port != VMADDR_PORT_ANY && !registry.ports.contains_key(&port) {
                    found = Some(port);
                    break;
                }
                port = if port >= u32::MAX - 1 {
                    49152
                } else {
                    port + 1
                };
            }
            let port = found.ok_or_else(|| {
                Error::with_message(Errno::EADDRINUSE, "no ephemeral port is available")
            })?;
            registry.next_port = if port >= u32::MAX - 1 {
                49152
            } else {
                port + 1
            };
            port
        } else {
            addr.port
        };
        if port < space.config.port_min.0 || port > space.config.port_max.0 {
            return_errno_with_message!(Errno::EACCES, "port is outside attachment policy")
        }
        if registry.ports.insert(port, 1).is_some() {
            return_errno_with_message!(Errno::EADDRINUSE, "vsock port is already bound")
        }
        drop(registry);
        Ok(Self {
            lease: Arc::new(PortLease { space, port }),
        })
    }
    pub(in crate::net::socket::vsock) fn new_ephemeral() -> Result<Self> {
        Self::new_exclusive(VsockSocketAddr {
            cid: VMADDR_CID_ANY,
            port: VMADDR_PORT_ANY,
        })
    }
    pub(in crate::net::socket::vsock) fn local_addr(&self) -> VsockSocketAddr {
        VsockSocketAddr {
            cid: self.lease.space.cid,
            port: self.lease.port,
        }
    }
    pub(in crate::net::socket::vsock) fn permits_peer(&self, cid: u32) -> bool {
        self.lease
            .space
            .config
            .peer_cids
            .iter()
            .any(|peer| peer.0 == cid as u64)
    }
    pub(in crate::net::socket::vsock) fn connect(
        self,
        remote: VsockSocketAddr,
        pollee: &Pollee,
    ) -> Result<Connection, (Error, Self)> {
        if !self.permits_peer(remote.cid) {
            return Err((
                Error::with_message(Errno::ENETUNREACH, "peer CID is not authorized"),
                self,
            ));
        }
        let space = self.lease.space.clone();
        let attempt = match identity(&space.next_attempt) {
            Ok(n) => AttemptId(n),
            Err(e) => return Err((e, self)),
        };
        let flow = Flow {
            source: address(self.local_addr()),
            destination: address(remote),
        };
        let incoming = flow.reversed();
        let conn = Conn::new(self, key(incoming, attempt), pollee.clone(), None);
        {
            let mut r = space.registry.lock();
            r.connections.retain(|_, c| {
                c.upgrade().is_some_and(|c| {
                    matches!(c.state.lock().phase, Phase::Connecting | Phase::Connected)
                })
            });
            if r.connections.len() >= space.config.max_flows as usize {
                return Err((
                    Error::with_message(Errno::ENOBUFS, "flow quota exhausted"),
                    conn.bound.clone(),
                ));
            }
            r.connections.insert(conn.key, Arc::downgrade(&conn));
        }
        if let Err(e) = space.send(VsockControlMessage {
            flow,
            attempt,
            kind: VsockControlKind::ConnectOffer,
        }) {
            space.registry.lock().connections.remove(&conn.key);
            return Err((e, conn.bound.clone()));
        }
        conn.arm_timeout();
        Ok(Connection { inner: Some(conn) })
    }
    pub(in crate::net::socket::vsock) fn listen(
        self,
        backlog: usize,
        pollee: &Pollee,
    ) -> Result<Listener, (Error, Self)> {
        let space = self.lease.space.clone();
        let port = self.lease.port;
        if space.removed.load(Ordering::Acquire) {
            return Err((
                Error::with_message(Errno::ENODEV, "TypedIO attachment removed"),
                self,
            ));
        }
        let inner = Arc::new(ListenerInner {
            bound: self,
            pollee: pollee.clone(),
            pending: SpinLock::new(Pending {
                backlog: backlog.min(space.config.max_flows as usize),
                connections: VecDeque::with_capacity(space.config.max_flows as usize),
            }),
        });
        let mut registry = space.registry.lock();
        if registry
            .listeners
            .get(&port)
            .is_some_and(|l| l.strong_count() != 0)
        {
            return Err((
                Error::with_message(Errno::EADDRINUSE, "listener already exists"),
                inner.bound.clone(),
            ));
        }
        registry.listeners.insert(port, Arc::downgrade(&inner));
        drop(registry);
        Ok(Listener { inner })
    }
}
impl Drop for PortLease {
    fn drop(&mut self) {
        self.space.registry.lock().ports.remove(&self.port);
    }
}

impl Conn {
    fn new(
        bound: BoundPort,
        key: Key,
        pollee: Pollee,
        listener: Option<Weak<ListenerInner>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            timeout: AtomicBool::new(false),
            bound,
            key,
            pollee,
            state: SpinLock::new(ConnState {
                phase: Phase::Connecting,
                error: None,
                tx: None,
                rx: None,
                tx_ready: false,
                rx_ready: false,
                local_read: false,
                local_write: false,
                peer_read: false,
                peer_write: false,
                timer: None,
            }),
            listener,
        })
    }
    fn flow(&self) -> Flow {
        Flow {
            source: address(self.bound.local_addr()),
            destination: Address {
                cid: ContextId(self.key.1),
                port: Port(self.key.2),
            },
        }
    }
    fn control(&self, kind: VsockControlKind) -> Result<()> {
        self.bound.lease.space.send(VsockControlMessage {
            flow: self.flow(),
            attempt: AttemptId(self.key.3),
            kind,
        })
    }
    fn fail(&self, error: Option<Error>) {
        let mut s = self.state.lock();
        let connecting = s.phase == Phase::Connecting;
        s.phase = if connecting {
            Phase::Failed
        } else {
            Phase::Closed
        };
        s.error = error;
        s.peer_read = true;
        s.peer_write = true;
        s.timer = None;
        let tx = s.tx.take();
        let rx = s.rx.take();
        drop(s);
        drop(tx);
        drop(rx);
        self.pollee
            .notify(IoEvents::IN | IoEvents::OUT | IoEvents::ERR | IoEvents::RDHUP | IoEvents::HUP);
        if let Some(listener) = self.listener.as_ref().and_then(Weak::upgrade) {
            listener.pollee.notify(IoEvents::IN)
        }
    }
    fn arm_timeout(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let timer =
            JIFFIES_TIMER_MANAGER
                .get()
                .unwrap()
                .create_timer(move |_guard: TimerGuard<'_>| {
                    if let Some(c) = weak.upgrade() {
                        c.timeout.store(true, Ordering::Release);
                        c.bound.lease.space.schedule();
                    }
                });
        timer
            .lock()
            .set_timeout(Timeout::After(Duration::from_secs(2)));
        let mut s = self.state.lock();
        if s.phase == Phase::Connecting {
            s.timer = Some(timer);
        }
    }
    fn reset(&self) {
        self.fail(Some(Error::with_message(
            Errno::ECONNRESET,
            "TypedIO vsock connection reset",
        )));
    }
    fn refresh(&self) {
        let mut s = self.state.lock();
        let mut notify = IoEvents::empty();
        if let Some(tx) = s.tx.as_mut() {
            let events = tx.take_events();
            if !tx.rearm_irq() {
                self.bound.lease.space.schedule();
            }
            if events.contains(ProducerEvents::SPACE_AVAILABLE) {
                s.tx_ready = true;
                notify |= IoEvents::OUT
            }
            if events.contains(ProducerEvents::CLOSED) {
                s.peer_read = true;
                notify |= IoEvents::OUT | IoEvents::HUP
            }
            if events.contains(ProducerEvents::RESET) {
                drop(s);
                self.reset();
                return;
            }
        }
        if let Some(rx) = s.rx.as_mut() {
            let events = rx.take_events();
            if !rx.rearm_irq() {
                self.bound.lease.space.schedule();
            }
            if events.contains(ConsumerEvents::DATA_AVAILABLE) {
                s.rx_ready = true;
                notify |= IoEvents::IN
            }
            if events.contains(ConsumerEvents::CLOSED) {
                s.peer_write = true;
                notify |= IoEvents::IN | IoEvents::RDHUP
            }
            if events.contains(ConsumerEvents::RESET) {
                drop(s);
                self.reset();
                return;
            }
        }
        drop(s);
        if !notify.is_empty() {
            self.pollee.notify(notify)
        }
    }
}
impl Connection {
    fn conn(&self) -> &Arc<Conn> {
        self.inner.as_ref().unwrap()
    }
    pub(in crate::net::socket::vsock) fn local_addr(&self) -> VsockSocketAddr {
        self.conn().bound.local_addr()
    }
    pub(in crate::net::socket::vsock) fn remote_addr(&self) -> VsockSocketAddr {
        VsockSocketAddr {
            cid: self.conn().key.1 as u32,
            port: self.conn().key.2,
        }
    }
    pub(in crate::net::socket::vsock) fn pollee(&self) -> &Pollee {
        &self.conn().pollee
    }
    pub(in crate::net::socket::vsock) fn test_and_clear_error(&self) -> Result<()> {
        self.conn().state.lock().error.take().map_or(Ok(()), Err)
    }
    pub(in crate::net::socket::vsock) fn has_connect_result(&self) -> bool {
        self.conn().state.lock().phase != Phase::Connecting
    }
    pub(in crate::net::socket::vsock) fn finish_connect(mut self) -> ConnectResult {
        let conn = self.inner.take().unwrap();
        let phase = conn.state.lock().phase;
        if phase == Phase::Failed {
            match Arc::try_unwrap(conn) {
                Ok(c) => {
                    let error = c.state.lock().error.take().unwrap_or_else(|| {
                        Error::with_message(Errno::ECONNRESET, "vsock connection failed")
                    });
                    ConnectResult::Failed(c.bound, error)
                }
                Err(c) => {
                    self.inner = Some(c);
                    ConnectResult::Connecting(self)
                }
            }
        } else {
            self.inner = Some(conn);
            if phase == Phase::Connecting {
                ConnectResult::Connecting(self)
            } else {
                ConnectResult::Connected(self)
            }
        }
    }
    pub(in crate::net::socket::vsock) fn check_io_events(&self) -> IoEvents {
        let s = self.conn().state.lock();
        let mut e = IoEvents::empty();
        if s.rx_ready || s.peer_write || s.local_read {
            e |= IoEvents::IN
        }
        if s.peer_write || s.local_read {
            e |= IoEvents::RDHUP
        }
        if !s.local_write && (s.tx_ready || s.peer_read) {
            e |= IoEvents::OUT
        }
        if (s.local_read && s.local_write) || (s.local_write && s.peer_write) {
            e |= IoEvents::HUP
        }
        if s.error.is_some() {
            e |= IoEvents::ERR
        }
        e
    }
    pub(in crate::net::socket::vsock) fn try_send(
        &mut self,
        reader: &mut dyn MultiRead,
        _flags: SendFlags,
    ) -> Result<usize> {
        if reader.sum_lens() == 0 {
            return Ok(0);
        }
        let conn = self.conn();
        let mut s = conn.state.lock();
        if let Some(error) = s.error.take() {
            return Err(error);
        }
        if s.local_write || s.peer_read || s.phase != Phase::Connected {
            return_errno_with_message!(Errno::EPIPE, "vsock is closed for writing")
        }
        let mut tx =
            s.tx.take()
                .ok_or_else(|| Error::with_message(Errno::EAGAIN, "producer is occupied"))?;
        s.tx_ready = false;
        drop(s);
        conn.pollee.invalidate();
        let result = (|| {
            let mut r = tx
                .try_reserve(reader.sum_lens().min(65536))
                .map_err(io_error)?;
            let result = r.produce_with(|dst| reader.read(dst));
            let n = match &result {
                Ok(n) | Err((_, n)) => *n,
            };
            r.commit(n).map_err(io_error)?;
            partial(result)
        })();
        let mut s = conn.state.lock();
        if !s.local_write && s.phase == Phase::Connected {
            s.tx = Some(tx)
        }
        drop(s);
        conn.pollee.invalidate();
        conn.bound.lease.space.schedule();
        result
    }
    pub(in crate::net::socket::vsock) fn try_recv(
        &mut self,
        writer: &mut dyn MultiWrite,
        flags: RecvFlags,
    ) -> Result<usize> {
        if writer.sum_lens() == 0 {
            return Ok(0);
        }
        let conn = self.conn();
        let mut s = conn.state.lock();
        if s.local_read {
            return Ok(0);
        }
        if let Some(error) = s.error.take() {
            return Err(error);
        }
        if s.phase == Phase::Closed {
            return Ok(0);
        }
        let mut rx =
            s.rx.take()
                .ok_or_else(|| Error::with_message(Errno::EAGAIN, "consumer is occupied"))?;
        s.rx_ready = false;
        drop(s);
        conn.pollee.invalidate();
        let consume = flags.receive_behavior().will_consume_data();
        let result = (|| {
            let Some(mut r) = rx
                .try_reserve(writer.sum_lens().min(65536))
                .map_err(io_error)?
            else {
                return Ok(0);
            };
            let result = r.consume_with(|src| writer.write(src));
            let n = match &result {
                Ok(n) | Err((_, n)) => *n,
            };
            if let Err(error) = r.commit(if consume { n } else { 0 }) {
                if n != 0 {
                    return Ok(n);
                }
                return Err(io_error(error));
            }
            partial(result)
        })();
        let mut s = conn.state.lock();
        if !s.local_read && s.phase == Phase::Connected {
            s.rx = Some(rx)
        }
        drop(s);
        conn.pollee.invalidate();
        conn.bound.lease.space.schedule();
        result
    }
    pub(in crate::net::socket::vsock) fn shutdown(&self, cmd: SockShutdownCmd) -> Result<()> {
        let conn = self.conn();
        let mut s = conn.state.lock();
        let read = cmd.shut_read() && !s.local_read;
        let write = cmd.shut_write() && !s.local_write;
        if !read && !write {
            return Ok(());
        }
        let direction = if read && write {
            ShutdownDirection::Both
        } else if read {
            ShutdownDirection::Receive
        } else {
            ShutdownDirection::Send
        };
        if read {
            s.local_read = true;
        }
        if write {
            s.local_write = true;
        }
        let tx = if write { s.tx.take() } else { None };
        let rx = if read { s.rx.take() } else { None };
        drop(s);
        drop(tx);
        drop(rx);
        conn.pollee
            .notify(IoEvents::IN | IoEvents::OUT | IoEvents::RDHUP | IoEvents::HUP);
        conn.control(VsockControlKind::Shutdown(direction))
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(conn) = self.inner.take() {
            let connecting = conn.state.lock().phase == Phase::Connecting;
            let _ = conn.control(if connecting {
                VsockControlKind::Reset
            } else {
                VsockControlKind::Shutdown(ShutdownDirection::Both)
            });
            conn.bound
                .lease
                .space
                .registry
                .lock()
                .connections
                .remove(&conn.key);
            conn.fail(None);
        }
    }
}
impl Listener {
    pub(in crate::net::socket::vsock) fn try_accept(&self) -> Result<Connection> {
        if self.inner.bound.lease.space.removed.load(Ordering::Acquire) {
            return_errno_with_message!(Errno::ENODEV, "TypedIO attachment removed")
        }
        let mut p = self.inner.pending.lock();
        p.connections
            .retain(|c| matches!(c.state.lock().phase, Phase::Connecting | Phase::Connected));
        let index = p
            .connections
            .iter()
            .position(|c| c.state.lock().phase == Phase::Connected)
            .ok_or_else(|| Error::with_message(Errno::EAGAIN, "no established vsock connection"))?;
        let c = p.connections.remove(index).unwrap();
        drop(p);
        self.inner.pollee.invalidate();
        Ok(Connection { inner: Some(c) })
    }
    pub(in crate::net::socket::vsock) fn set_backlog(&self, n: usize) {
        self.inner.pending.lock().backlog =
            n.min(self.inner.bound.lease.space.config.max_flows as usize)
    }
    pub(in crate::net::socket::vsock) fn local_addr(&self) -> VsockSocketAddr {
        self.inner.bound.local_addr()
    }
    pub(in crate::net::socket::vsock) fn check_io_events(&self) -> IoEvents {
        if self.inner.bound.lease.space.removed.load(Ordering::Acquire) {
            return IoEvents::ERR | IoEvents::HUP;
        }
        if self
            .inner
            .pending
            .lock()
            .connections
            .iter()
            .any(|c| c.state.lock().phase == Phase::Connected)
        {
            IoEvents::IN
        } else {
            IoEvents::empty()
        }
    }
}
impl Drop for Listener {
    fn drop(&mut self) {
        self.inner
            .bound
            .lease
            .space
            .registry
            .lock()
            .listeners
            .remove(&self.inner.bound.lease.port);
        let pending = core::mem::take(&mut self.inner.pending.lock().connections);
        for c in pending {
            let _ = c.control(VsockControlKind::Reset);
            c.bound
                .lease
                .space
                .registry
                .lock()
                .connections
                .remove(&c.key);
            c.fail(None);
        }
    }
}

impl Space {
    fn schedule(&self) {
        if let Some(work) = self.work.get() {
            work.schedule()
        }
    }
    fn send(&self, message: VsockControlMessage) -> Result<()> {
        if self.removed.load(Ordering::Acquire) {
            return_errno_with_message!(Errno::ENODEV, "TypedIO attachment was removed")
        }
        let tag = identity(&self.next_tag)?;
        let mut r = self.registry.lock();
        if r.controls.len() >= CONTROL_LIMIT {
            return_errno_with_message!(Errno::EAGAIN, "control queue is full")
        }
        r.controls.insert(tag, message);
        r.pending.push_back((tag, message));
        drop(r);
        self.schedule();
        Ok(())
    }
    fn event(&self, event: VsockEvent) {
        match event {
            VsockEvent::Control(message) => {
                if message.flow.destination.cid.0 != self.cid as u64 {
                    return;
                }
                if message.kind == VsockControlKind::ConnectOffer {
                    self.offer(message);
                    return;
                }
                let conn = self
                    .registry
                    .lock()
                    .connections
                    .get(&key(message.flow, message.attempt))
                    .and_then(Weak::upgrade);
                let Some(conn) = conn else { return };
                match message.kind {
                    VsockControlKind::Reset => {
                        conn.reset();
                    }
                    VsockControlKind::Shutdown(direction) => {
                        let mut s = conn.state.lock();
                        if matches!(direction, ShutdownDirection::Send | ShutdownDirection::Both) {
                            s.peer_write = true
                        }
                        if matches!(
                            direction,
                            ShutdownDirection::Receive | ShutdownDirection::Both
                        ) {
                            s.peer_read = true
                        }
                        drop(s);
                        conn.pollee
                            .notify(IoEvents::IN | IoEvents::OUT | IoEvents::RDHUP | IoEvents::HUP)
                    }
                    _ => (),
                }
            }
            VsockEvent::Established {
                flow,
                attempt,
                mut tx,
                mut rx,
            } => {
                if let Some(irq) = self.irq.get() {
                    if tx.bind_irq(irq).is_err() || rx.bind_irq(irq).is_err() {
                        let _ = self.send(VsockControlMessage {
                            flow: flow.reversed(),
                            attempt,
                            kind: VsockControlKind::Reset,
                        });
                        return;
                    }
                }
                let _ = tx.set_interest(ProducerInterest::SPACE_AVAILABLE);
                let _ = rx.set_interest(ConsumerInterest::DATA_AVAILABLE);
                let conn = self
                    .registry
                    .lock()
                    .connections
                    .get(&key(flow, attempt))
                    .and_then(Weak::upgrade);
                if let Some(conn) = conn {
                    let mut s = conn.state.lock();
                    if s.phase == Phase::Connecting && flow.destination.cid.0 == self.cid as u64 {
                        s.tx = Some(tx);
                        s.rx = Some(rx);
                        s.phase = Phase::Connected;
                        s.timer = None;
                        drop(s);
                        conn.refresh();
                        conn.pollee.notify(IoEvents::OUT);
                        if let Some(listener) = conn.listener.as_ref().and_then(Weak::upgrade) {
                            listener.pollee.notify(IoEvents::IN)
                        }
                        return;
                    }
                }
                let _ = self.send(VsockControlMessage {
                    flow: flow.reversed(),
                    attempt,
                    kind: VsockControlKind::Reset,
                });
            }
        }
    }
    fn offer(&self, message: VsockControlMessage) {
        let listener = self
            .registry
            .lock()
            .listeners
            .get(&message.flow.destination.port.0)
            .and_then(Weak::upgrade);
        let Some(listener) = listener else {
            let _ = self.send(VsockControlMessage {
                flow: message.flow.reversed(),
                attempt: message.attempt,
                kind: VsockControlKind::Reset,
            });
            return;
        };
        let mut pending = listener.pending.lock();
        pending
            .connections
            .retain(|c| matches!(c.state.lock().phase, Phase::Connecting | Phase::Connected));
        if pending.connections.len() >= pending.backlog {
            drop(pending);
            let _ = self.send(VsockControlMessage {
                flow: message.flow.reversed(),
                attempt: message.attempt,
                kind: VsockControlKind::Reset,
            });
            return;
        }
        let conn = Conn::new(
            listener.bound.clone(),
            key(message.flow, message.attempt),
            Pollee::new(),
            Some(Arc::downgrade(&listener)),
        );
        pending.connections.push_back(conn.clone());
        drop(pending);
        self.registry
            .lock()
            .connections
            .insert(conn.key, Arc::downgrade(&conn));
        if self
            .send(VsockControlMessage {
                flow: message.flow.reversed(),
                attempt: message.attempt,
                kind: VsockControlKind::ConnectAccept,
            })
            .is_err()
        {
            conn.fail(Some(Error::with_message(
                Errno::ENOBUFS,
                "control admission failed",
            )));
        } else {
            conn.arm_timeout();
        }
    }
    fn service(&self) {
        if self.busy.swap(true, Ordering::Acquire) {
            return;
        }
        struct Guard<'a>(&'a AtomicBool);
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release)
            }
        }
        let _guard = Guard(&self.busy);
        let events = self.receive.take_events() | self.control.take_events();
        if events.contains(QueueEvents::REMOVED) {
            self.removed.store(true, Ordering::Release);
        }
        for _ in 0..64 {
            let Some(completion) = self.control.try_pop_completion() else {
                break;
            };
            let message = self.registry.lock().controls.remove(&completion.tag.0);
            if completion.result.is_err() {
                if let Some(message) = message {
                    let incoming = message.flow.reversed();
                    if let Some(conn) = self
                        .registry
                        .lock()
                        .connections
                        .get(&key(incoming, message.attempt))
                        .and_then(Weak::upgrade)
                    {
                        conn.fail(Some(Error::with_message(
                            Errno::ECONNRESET,
                            "vsock control failed",
                        )));
                    }
                }
            }
        }
        for _ in 0..64 {
            let Some(completion) = self.receive.try_pop_completion() else {
                break;
            };
            self.posted.fetch_sub(1, Ordering::Relaxed);
            if let Ok(VsockReply::Event(event)) = completion.result {
                self.event(event)
            }
        }
        if !self.removed.load(Ordering::Acquire) {
            for _ in 0..CONTROL_LIMIT {
                let item = self.registry.lock().pending.pop_front();
                let Some((tag, message)) = item else { break };
                match self
                    .control
                    .try_submit(VsockRequest::SendControl(message), RequestTag(tag))
                {
                    Ok(_) => (),
                    Err(error) => {
                        self.registry.lock().pending.push_front((tag, message));
                        if !matches!(error.reason, typedio::SubmitReason::WouldBlock) {
                            self.removed.store(true, Ordering::Release);
                        }
                        break;
                    }
                }
            }
            while self.posted.load(Ordering::Relaxed) < 8 {
                let Ok(tag) = identity(&self.next_tag) else {
                    break;
                };
                if self
                    .receive
                    .try_submit(VsockRequest::ReceiveEvent, RequestTag(tag))
                    .is_err()
                {
                    break;
                }
                self.posted.fetch_add(1, Ordering::Relaxed);
            }
        }
        let listeners: Vec<_> = self
            .registry
            .lock()
            .listeners
            .values()
            .filter_map(Weak::upgrade)
            .collect();
        for listener in &listeners {
            listener
                .pending
                .lock()
                .connections
                .retain(|c| matches!(c.state.lock().phase, Phase::Connecting | Phase::Connected));
        }
        self.registry.lock().connections.retain(|_, c| {
            c.upgrade().is_some_and(|c| {
                matches!(c.state.lock().phase, Phase::Connecting | Phase::Connected)
            })
        });
        let connections: Vec<_> = self
            .registry
            .lock()
            .connections
            .values()
            .filter_map(Weak::upgrade)
            .collect();
        for c in connections {
            if c.timeout.swap(false, Ordering::AcqRel) && c.state.lock().phase == Phase::Connecting
            {
                c.fail(Some(Error::with_message(
                    Errno::ETIMEDOUT,
                    "vsock connect timed out",
                )));
                let _ = c.control(VsockControlKind::Reset);
            }
            if self.removed.load(Ordering::Acquire) {
                c.fail(Some(Error::with_message(
                    Errno::ENODEV,
                    "TypedIO attachment removed",
                )))
            } else {
                c.refresh()
            }
        }
        if self.removed.load(Ordering::Acquire) {
            for listener in listeners {
                listener.pending.lock().connections.clear();
                listener
                    .pollee
                    .notify(IoEvents::IN | IoEvents::ERR | IoEvents::HUP);
            }
        }
        let more = !self.receive.rearm_irq() | !self.control.rearm_irq();
        if more && !self.removed.load(Ordering::Acquire) {
            self.schedule()
        }
    }
}
pub(in crate::net::socket::vsock) fn init() {
    let devices = match typedio::discover::<VsockProtocol>() {
        Ok(d) => d,
        Err(_) => return,
    };
    let mut spaces = Vec::new();
    for device in devices {
        let Ok(cid) = u32::try_from(device.config().cid.0) else {
            continue;
        };
        let Ok(control) = device.open(QueueOptions {
            depth: 8,
            features: 0,
        }) else {
            continue;
        };
        let Ok(receive) = device.open(QueueOptions {
            depth: 8,
            features: 0,
        }) else {
            continue;
        };
        let space = Arc::new(Space {
            cid,
            config: device.config().clone(),
            control,
            receive,
            registry: SpinLock::new(Registry {
                ports: BTreeMap::new(),
                next_port: 49152,
                connections: BTreeMap::new(),
                listeners: BTreeMap::new(),
                controls: BTreeMap::new(),
                pending: VecDeque::with_capacity(CONTROL_LIMIT),
            }),
            next_tag: AtomicU64::new(1),
            next_attempt: AtomicU64::new(1),
            posted: AtomicUsize::new(0),
            removed: AtomicBool::new(false),
            busy: AtomicBool::new(false),
            work: Once::new(),
            irq: Once::new(),
            #[cfg(feature = "typedio-polling")]
            timer: Once::new(),
        });
        let weak = Arc::downgrade(&space);
        space.work.call_once(|| {
            Taskless::new(move || {
                if let Some(space) = weak.upgrade() {
                    space.service()
                }
            })
        });
        #[cfg(not(feature = "typedio-polling"))]
        {
            let Ok(mut irq) = IrqLine::alloc() else {
                continue;
            };
            let weak = Arc::downgrade(&space);
            irq.on_active(move |_| {
                if let Some(space) = weak.upgrade() {
                    space.schedule();
                }
            });
            if space.control.bind_irq(&irq).is_err() || space.receive.bind_irq(&irq).is_err() {
                continue;
            }
            space.irq.call_once(|| irq);
        }
        #[cfg(feature = "typedio-polling")]
        {
            let weak = Arc::downgrade(&space);
            let timer = JIFFIES_TIMER_MANAGER.get().unwrap().create_timer(
                move |mut guard: TimerGuard<'_>| {
                    if let Some(space) = weak.upgrade() {
                        space.schedule();
                        if !space.removed.load(Ordering::Acquire) {
                            guard.set_timeout(Timeout::After(POLL_PERIOD));
                        }
                    }
                },
            );
            timer.lock().set_timeout(Timeout::After(POLL_PERIOD));
            space.timer.call_once(|| timer);
        }
        space.schedule();
        spaces.push(space);
    }
    DEVICES.call_once(|| spaces);
}
