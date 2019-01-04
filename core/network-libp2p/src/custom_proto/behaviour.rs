// Copyright 2018 Parity Technologies (UK) Ltd.
// This file is part of Substrate.

// Substrate is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Substrate is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Substrate.  If not, see <http://www.gnu.org/licenses/>.

use crate::custom_proto::handler::{CustomProtosHandler, CustomProtosHandlerOut, CustomProtosHandlerIn};
use crate::custom_proto::upgrade::RegisteredProtocols;
use crate::{NetworkConfiguration, NonReservedPeerMode, ProtocolId, topology::NetTopology};
use bytes::Bytes;
use fnv::FnvHashSet;
use futures::prelude::*;
use libp2p::core::swarm::{ConnectedPoint, NetworkBehaviour, NetworkBehaviourAction, PollParameters};
use libp2p::core::{protocols_handler::ProtocolsHandler, Endpoint, PeerId};
use smallvec::SmallVec;
use std::{marker::PhantomData, time::Instant};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_timer::Delay;

/// Network behaviour that handles opening substreams for custom protocols with other nodes.
pub struct CustomProtos<TSubstream> {
	/// List of protocols to open with peers. Never modified.
	registered_protocols: RegisteredProtocols,

	/// List of protocols that we have open with remotes.
	open_protocols: Vec<(PeerId, ProtocolId, Endpoint)>,

	/// Maximum number of incoming non-reserved connections, taken from the config.
	// TODO: unused
	max_incoming_connections: usize,

	/// Maximum number of outgoing non-reserved connections, taken from the config.
	max_outgoing_connections: usize,

	/// If true, only reserved peers can connect. TODO: shouldn't be here?
	reserved_only: bool,

	/// List of the IDs of the reserved peers. We always try to maintain a connection these peers.
	reserved_peers: FnvHashSet<PeerId>,

	/// When this delay expires, we need to synchronize our active connectons with the
	/// network topology.
	next_connect_to_nodes: Delay,

	/// Events to produce from `poll()`.
	events: SmallVec<[NetworkBehaviourAction<CustomProtosHandlerIn, (PeerId, CustomProtosHandlerOut)>; 4]>,

	/// Marker to pin the generics.
	marker: PhantomData<TSubstream>,
}

impl<TSubstream> CustomProtos<TSubstream> {
	/// Creates a `CustomProtos`.
	pub fn new(config: &NetworkConfiguration, registered_protocols: RegisteredProtocols) -> Self {
		let max_incoming_connections = config.in_peers as usize;
		let max_outgoing_connections = config.out_peers as usize;

		let open_protos_cap = max_incoming_connections
			.saturating_add(max_outgoing_connections)
			.saturating_mul(registered_protocols.len());

		CustomProtos {
			registered_protocols,
			max_incoming_connections,
			max_outgoing_connections,
			reserved_only: config.non_reserved_mode == NonReservedPeerMode::Deny,
			reserved_peers: Default::default(),
			open_protocols: Vec::with_capacity(open_protos_cap),
			next_connect_to_nodes: Delay::new(Instant::now()),
			events: SmallVec::new(),
			marker: PhantomData,
		}
	}

	/// Try to add a reserved peer.
	pub fn add_reserved_peer(&mut self, peer_id: PeerId) {
		self.reserved_peers.insert(peer_id);

		// Trigger a `connect_to_nodes` round.
		self.next_connect_to_nodes = Delay::new(Instant::now());
	}

	/// Try to remove a reserved peer.
	///
	/// If we are in reserved mode and we were connected to a node with this peer ID, then this
	/// method will disconnect it and return its index.
	pub fn remove_reserved_peer(&mut self, peer_id: PeerId) {
		self.reserved_peers.remove(&peer_id);
	}

	/// Start accepting all peers again if we weren't.
	pub fn accept_unreserved_peers(&mut self) {
		if !self.reserved_only {
			return
		}

		self.reserved_only = false;

		// Trigger a `connect_to_nodes` round.
		self.next_connect_to_nodes = Delay::new(Instant::now());
	}

	/// Start refusing non-reserved nodes. Returns the list of nodes that have been disconnected.
	pub fn deny_unreserved_peers(&mut self) {
		if self.reserved_only {
			return
		}

		self.reserved_only = true;

		// Disconnecting nodes that are connected to us and that aren't reserved
		for (peer_id, _, _) in &self.open_protocols {
			if self.reserved_peers.contains(peer_id) {
				continue
			}

			self.events.push(NetworkBehaviourAction::SendEvent {
				peer_id: peer_id.clone(),
				event: CustomProtosHandlerIn::Disable,
			});
		}
	}

	/// Disconnects the given peer if we are connected to it.
	pub fn disconnect_peer(&mut self, peer: &PeerId) {
		self.events.push(NetworkBehaviourAction::SendEvent {
			peer_id: peer.clone(),
			event: CustomProtosHandlerIn::Disable,
		});
	}

	/// Sends a message to a peer using the given custom protocol.
	///
	/// Has no effect if the custom protocol is not open with the given peer.
	///
	/// Also note that even we have a valid open substream, it may in fact be already closed
	/// without us knowing, in which case the packet will not be received.
	pub fn send_packet(&mut self, target: &PeerId, protocol_id: ProtocolId, data: impl Into<Bytes>) {
		self.events.push(NetworkBehaviourAction::SendEvent {
			peer_id: target.clone(),
			event: CustomProtosHandlerIn::SendCustomMessage {
				protocol: protocol_id,
				data: data.into(),
			}
		});
	}

	/// Updates the attempted connections to nodes.
	///
	/// Also updates `next_connect_to_nodes` with the earliest known moment when we need to
	/// update connections again.
	fn connect_to_nodes(&mut self, params: &mut PollParameters<NetTopology>) {
		// Make sure we are connected or connecting to all the reserved nodes.
		for reserved in self.reserved_peers.iter() {
			// TODO: only if not in a pending connection
			if self.open_protocols.iter().all(|(p, _, _)| p != reserved) {
				self.events.push(NetworkBehaviourAction::DialPeer { peer_id: reserved.clone() });
			}
		}

		// We're done with reserved node; return early if there's nothing more to do.
		if self.reserved_only {
			return;
		}

		// Counter of number of connections to open, decreased when we open one.
		let mut num_to_open = {
			let num_outgoing_connections = self.open_protocols
				.iter()
				.filter(|(_, _, endpoint)| endpoint == &Endpoint::Dialer)
				.count();
			self.max_outgoing_connections - num_outgoing_connections
		};

		let local_peer_id = params.local_peer_id().clone();
		let (to_try, will_change) = params.topology().addrs_to_attempt();
		for (peer_id, addr) in to_try {
			if num_to_open == 0 {
				break;
			}

			if peer_id == &local_peer_id {
				continue;
			}

			// TODO: restore
			/*if self.disabled_peers.contains_key(&peer_id) {
				continue;
			}

			// It is possible that we are connected to this peer, but the topology doesn't know
			// about that because it is an incoming connection.
			match self.swarm.ensure_connection(peer_id.clone(), addr.clone()) {
				Ok(true) => (),
				Ok(false) => num_to_open -= 1,
				Err(_) => ()
			}*/
		}

		self.next_connect_to_nodes.reset(will_change);
	}
}

impl<TSubstream> NetworkBehaviour<NetTopology> for CustomProtos<TSubstream>
where
	TSubstream: AsyncRead + AsyncWrite,
{
	type ProtocolsHandler = CustomProtosHandler<TSubstream>;
	type OutEvent = (PeerId, CustomProtosHandlerOut);

	fn new_handler(&mut self) -> Self::ProtocolsHandler {
		CustomProtosHandler::new(self.registered_protocols.clone())
	}

	fn inject_connected(&mut self, _: PeerId, _: ConnectedPoint) {
	}

	fn inject_disconnected(&mut self, peer_id: &PeerId, _: ConnectedPoint) {
		if let Some(pos) = self.open_protocols.iter().position(|(p, _, _)| p == peer_id) {
			let (_, protocol_id, _) = self.open_protocols.remove(pos);
			let event = CustomProtosHandlerOut::CustomProtocolClosed {
				protocol_id,
				result: Ok(()),
			};
			self.events.push(NetworkBehaviourAction::GenerateEvent((peer_id.clone(), event)));
		}
	}

	fn inject_node_event(
		&mut self,
		source: PeerId,
		event: <Self::ProtocolsHandler as ProtocolsHandler>::OutEvent,
	) {
		match event {
			CustomProtosHandlerOut::CustomProtocolClosed { ref protocol_id, .. } => {
				let pos = self.open_protocols.iter().position(|(s, p, _)| {
					s == &source && p == protocol_id
				});

				if let Some(pos) = pos {
					self.open_protocols.remove(pos);
				} else {
					debug_assert!(false, "Couldn't find protocol in open_protocols");
				}
			}
			CustomProtosHandlerOut::CustomProtocolOpen { ref protocol_id, endpoint, .. } => {
				debug_assert!(!self.open_protocols.iter().any(|(s, p, _)| {
					s == &source && p == protocol_id
				}));
				self.open_protocols.push((source.clone(), *protocol_id, endpoint));
			}
			_ => {}
		}

		self.events.push(NetworkBehaviourAction::GenerateEvent((source, event)));
	}

	fn poll(
		&mut self,
		params: &mut PollParameters<NetTopology>,
	) -> Async<
		NetworkBehaviourAction<
			<Self::ProtocolsHandler as ProtocolsHandler>::InEvent,
			Self::OutEvent,
		>,
	> {
		loop {
			match self.next_connect_to_nodes.poll() {
				Ok(Async::Ready(())) => self.connect_to_nodes(params),
				Ok(Async::NotReady) => break,
				Err(err) => {
					warn!(target: "sub-libp2p", "Connect-to-nodes timer errored: {:?}", err);
					break;
				}
			}
		}

		if !self.events.is_empty() {
			return Async::Ready(self.events.remove(0));
		}

		Async::NotReady
	}
}
