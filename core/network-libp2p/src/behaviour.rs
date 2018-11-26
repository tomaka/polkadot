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

use crate::connec_limit::ConnecLimitBehaviour;
use crate::custom_proto::{CustomProtosBehaviour, RegisteredProtocols};
use crate::NetworkConfiguration;
use libp2p::core::{Multiaddr, PeerId};
use libp2p::identify::{PeriodicIdentification, IdentifyListen, IdentifyInfo};
use libp2p::kad::Kademlia;
use libp2p::ping::{PeriodicPingHandler, PingListenHandler};

/// General behaviour of the network.
#[derive(NetworkBehaviour)]
pub struct Behaviour<TSubstream> {
	/// Periodically ping nodes, and close the connection if it's unresponsive.
	periodic_ping: PeriodicPingHandler<TSubstream>,
	/// Respond to incoming pings.
	ping_listen: PingListenHandler<TSubstream>,
	/// Enforces disabled and reserved peers, and connection limit.
	limiter: ConnecLimitBehaviour<TSubstream>,
	/// Custom protocols (dot, bbq, sub, etc.).
	custom_protocols: CustomProtosBehaviour<TSubstream>,
	/// Kademlia requests and answers.
	kademlia: Kademlia<TSubstream>,
	/// Periodically identifies the remote.
	periodic_identify: PeriodicIdentification<TSubstream>,
	///// Respond to identify requests.
	//identify_listen: IdentifyListen<TSubstream>,
}

impl<TSubstream> Behaviour<TSubstream> {
	/// Builds a new `Behaviour`.
	// TODO: redundancy between config and local_peer_id
	pub fn new(config: &NetworkConfiguration, local_peer_id: PeerId, protocols: RegisteredProtocols) -> Self {
		// TODO:
		/*let id_info = IdentifyInfo {

		};*/

		Behaviour {
			periodic_ping: PeriodicPingHandler::new(),
			ping_listen: PingListenHandler::new(),
			limiter: ConnecLimitBehaviour::new(config),
			custom_protocols: CustomProtosBehaviour::new(protocols),
			kademlia: Kademlia::new(local_peer_id),
			periodic_identify: PeriodicIdentification::new(),
			//identify_listen: IdentifyListen::new(id_info),
		}
	}

	/// Starts a Kademlia `FIND_NODE` query for a random `PeerId`.
	///
	/// Used to discover nodes.
	pub fn perform_kad_random_query(&mut self) {
		let random_peer_id = PeerId::random();
		self.kademlia.find_node(random_peer_id);
	}

	/// Try to add a reserved peer.
	pub fn add_reserved_peer(&mut self, peer_id: PeerId, addr: Multiaddr) {
		self.limiter.add_reserved_peer(peer_id, addr)
	}

	/// Try to remove a reserved peer.
	///
	/// If we are in reserved mode and we were connected to a node with this peer ID, then this
	/// method will disconnect it and return its index.
	pub fn remove_reserved_peer(&mut self, peer_id: PeerId) {
		self.limiter.remove_reserved_peer(peer_id)
	}

	/// Start accepting all peers again if we weren't.
	pub fn accept_unreserved_peers(&mut self) {
		self.limiter.accept_unreserved_peers()
	}

	/// Start refusing non-reserved nodes. Returns the list of nodes that have been disconnected.
	pub fn deny_unreserved_peers(&mut self) {
		self.limiter.deny_unreserved_peers()
	}

	/// Disconnects a peer and bans it for a little while.
	///
	/// Same as `drop_node`, except that the same peer will not be able to reconnect later.
	#[inline]
	pub fn ban_node(&mut self, peer_id: PeerId) {
		self.limiter.ban_node(peer_id);
	}

	/// Disconnects a peer.
	///
	/// This is asynchronous and will not immediately close the peer.
	/// Corresponding closing events will be generated once the closing actually happens.
	///
	/// Has no effect if we're not connected to the `PeerId`.
	#[inline]
	pub fn drop_node(&mut self, peer_id: &PeerId) {
		self.limiter.drop_node(&peer_id);
	}
}


// TODO: remove or put in tests module

fn requires_net_behaviour<T: ::libp2p::core::nodes::NetworkBehaviour<::topology::NetTopology>>() {
}

fn test() {
	requires_net_behaviour::<PeriodicPingHandler<::libp2p::core::nodes::Substream<::libp2p::core::muxing::StreamMuxerBox>>>();
}
