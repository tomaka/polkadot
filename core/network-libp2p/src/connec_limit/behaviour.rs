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

use crate::{NetworkConfiguration, NonReservedPeerMode};
use crate::connec_limit::handler::ConnecLimitHandler;
use crate::topology::DisconnectReason;
use fnv::{FnvHashMap, FnvHashSet};
use futures::prelude::*;
use libp2p::core::swarm::{ConnectedPoint, NetworkBehaviour, NetworkBehaviourAction, PollParameters};
use libp2p::core::{protocols_handler::ProtocolsHandler, Multiaddr, PeerId};
use std::{marker::PhantomData, time::Duration, time::Instant};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_timer::Delay;
use void::Void;

// Duration during which a peer is disabled.
const PEER_DISABLE_DURATION: Duration = Duration::from_secs(5 * 60);

/// Network behaviour that handles opening substreams for custom protocols with other nodes.
// TODO: does it need a generic?
pub struct ConnecLimitBehaviour<TSubstream> {
	/// Maximum number of incoming non-reserved connections, taken from the config.
	max_incoming_connections: usize,

	/// Maximum number of outgoing non-reserved connections, taken from the config.
	max_outgoing_connections: usize,

	/// If true, only reserved peers can connect.
	reserved_only: bool,

	/// List of the IDs of the reserved peers.
	reserved_peers: FnvHashSet<PeerId>,

	/// List of the IDs of disabled peers, and when the ban expires.
	/// Purged at a regular interval.
	// TODO: purge at a regular interval
	disabled_peers: FnvHashMap<PeerId, Instant>,

	/// Future that will fire when we need to connect to new nodes.
	next_connect_to_nodes: Delay,

	/// Marker to pin the generics.
	marker: PhantomData<TSubstream>,
}

impl<TSubstream> ConnecLimitBehaviour<TSubstream> {
	/// Creates a `ConnecLimitBehaviour`.
	pub fn new(config: &NetworkConfiguration) -> Self {
		ConnecLimitBehaviour {
			max_incoming_connections: config.in_peers as usize,
			max_outgoing_connections: config.out_peers as usize,
			reserved_only: config.non_reserved_mode == NonReservedPeerMode::Deny,
			reserved_peers: Default::default(),
			disabled_peers: Default::default(),
			next_connect_to_nodes: Delay::new(Instant::now()),
			marker: PhantomData,
		}
	}

	// TODO: implement all the methods bodies
	
	/// Try to add a reserved peer.
	pub fn add_reserved_peer(&mut self, peer_id: PeerId, addr: Multiaddr) {
		self.reserved_peers.insert(peer_id.clone());
		/*self.topology.add_bootstrap_addr(&peer_id, addr.clone());
		let _ = self.swarm.ensure_connection(peer_id, addr);*/
	}

	/// Try to remove a reserved peer.
	///
	/// If we are in reserved mode and we were connected to a node with this peer ID, then this
	/// method will disconnect it and return its index.
	pub fn remove_reserved_peer(&mut self, peer_id: PeerId) /*-> Option<NodeIndex>*/ {
		self.reserved_peers.remove(&peer_id);
		/*if self.reserved_only {
			if let Some(node_index) = self.swarm.latest_node_by_peer_id(&peer_id) {
				self.drop_node_inner(node_index, DisconnectReason::NoSlot, None);
				return Some(node_index);
			}
		}
		None*/
	}

	/// Start accepting all peers again if we weren't.
	pub fn accept_unreserved_peers(&mut self) {
		/*if self.reserved_only {
			self.reserved_only = false;
			self.connect_to_nodes();
		}*/
	}

	/// Start refusing non-reserved nodes. Returns the list of nodes that have been disconnected.
	pub fn deny_unreserved_peers(&mut self) /*-> Vec<NodeIndex>*/ {
		/*self.reserved_only = true;

		// Disconnect the nodes that are not reserved.
		let to_disconnect: Vec<NodeIndex> = self.swarm
			.nodes()
			.filter(|&n| {
				let peer_id = self.swarm.peer_id_of_node(n)
					.expect("swarm.nodes() always returns valid node indices");
				!self.reserved_peers.contains(peer_id)
			})
			.collect();

		for &node_index in &to_disconnect {
			self.drop_node_inner(node_index, DisconnectReason::NoSlot, None);
		}

		to_disconnect*/
	}

	/// Disconnects a peer and bans it for a little while.
	///
	/// Same as `drop_node`, except that the same peer will not be able to reconnect later.
	#[inline]
	pub fn ban_node(&mut self, peer_id: PeerId) {
		self.drop_node_inner(&peer_id, DisconnectReason::Banned, Some(PEER_DISABLE_DURATION));
	}

	/// Disconnects a peer.
	///
	/// This is asynchronous and will not immediately close the peer.
	/// Corresponding closing events will be generated once the closing actually happens.
	#[inline]
	pub fn drop_node(&mut self, peer_id: &PeerId) {
		self.drop_node_inner(peer_id, DisconnectReason::Useless, None);
	}

	/// Common implementation of `drop_node` and `ban_node`.
	fn drop_node_inner(
		&mut self,
		peer_id: &PeerId,
		reason: DisconnectReason,
		disable_duration: Option<Duration>
	) {
		// TODO:
		/*// Kill the node from the swarm, and inject an event about it.
		// TODO: actually do that
		if let Some(ConnectedPoint::Dialer { address }) = self.nodes_addresses.remove(&node_index) {
			self.topology.report_disconnected(&address, reason);
		}

		if let Some(disable_duration) = disable_duration {
			let timeout = Instant::now() + disable_duration;
			self.disabled_peers.insert(peer_id, timeout);
		}

		self.connect_to_nodes();*/
	}
}

impl<TSubstream, TTopology> NetworkBehaviour<TTopology> for ConnecLimitBehaviour<TSubstream>
where
	TSubstream: AsyncRead + AsyncWrite,
{
	type ProtocolsHandler = ConnecLimitHandler<TSubstream>;
	type OutEvent = Void;

	fn new_handler(&mut self) -> Self::ProtocolsHandler {
		ConnecLimitHandler::new()
	}

	fn inject_connected(&mut self, _: PeerId, _: ConnectedPoint) {}

	// TODO: send event for closed protocols
	fn inject_disconnected(&mut self, _: &PeerId, _: ConnectedPoint) {}

	fn inject_node_event(
		&mut self,
		_: PeerId,
		_: <Self::ProtocolsHandler as ProtocolsHandler>::OutEvent,
	) {
	}

	fn poll(
		&mut self,
		_: &mut PollParameters<TTopology>,
	) -> Async<
		NetworkBehaviourAction<
			<Self::ProtocolsHandler as ProtocolsHandler>::InEvent,
			Self::OutEvent,
		>,
	> {
		Async::NotReady
	}
}
