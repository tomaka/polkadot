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

use crate::{behaviour::Behaviour, secret::obtain_private_key, transport};
use bytes::Bytes;
use custom_proto::{RegisteredProtocol, RegisteredProtocols};
use fnv::{FnvHashMap, FnvHashSet};
use futures::{prelude::*, Stream};
use libp2p::{Multiaddr, PeerId};
use libp2p::core::{Endpoint, Swarm};
use libp2p::core::{nodes::Substream, transport::boxed::Boxed, muxing::StreamMuxerBox};
use libp2p::core::nodes::ConnectedPoint;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};
use topology::{DisconnectReason, NetTopology};
use tokio_timer::{Delay, Interval};
use {Error, ErrorKind, NetworkConfiguration, NodeIndex, ProtocolId, parse_str_addr};

// File where the network topology is stored.
const NODES_FILE: &str = "nodes.json";

/// Starts the substrate libp2p service.
///
/// Returns a stream that must be polled regularly in order for the networking to function.
pub fn start_service<TProtos>(
	config: NetworkConfiguration,
	registered_custom: TProtos,
) -> Result<Service, Error>
where TProtos: IntoIterator<Item = RegisteredProtocol> {
	// Private and public keys configuration.
	let local_private_key = obtain_private_key(&config)?;
	let local_public_key = local_private_key.to_public_key();
	let local_peer_id = local_public_key.clone().into_peer_id();

	// Initialize the topology of the network.
	let mut topology = if let Some(ref path) = config.net_config_path {
		let path = Path::new(path).join(NODES_FILE);
		debug!(target: "sub-libp2p", "Initializing peer store for JSON file {:?}", path);
		NetTopology::from_file(path)
	} else {
		debug!(target: "sub-libp2p", "No peers file configured ; peers won't be saved");
		NetTopology::memory()
	};

	// Build the swarm.
	let swarm = {
		let registered_custom = RegisteredProtocols(registered_custom.into_iter().collect());
		let behaviour = Behaviour::new(&config, local_peer_id.clone(), registered_custom);
		let transport = transport::build_transport(local_private_key);
		Swarm::new(transport, behaviour, topology)
	};

	// Listen on multiaddresses.
	for addr in &config.listen_addresses {
		match Swarm::listen_on(&mut swarm, addr.clone()) {
			Ok(new_addr) => debug!(target: "sub-libp2p", "Libp2p listening on {}", new_addr),
			Err(_) => {
				warn!(target: "sub-libp2p", "Can't listen on {}, protocol not supported", addr);
				return Err(ErrorKind::BadProtocol.into())
			},
		}
	}

	// TODO: restore
	/*// Register the external addresses provided by the user.
	for addr in &config.public_addresses {
		swarm.add_external_address(addr.clone());
	}*/

	// Add the bootstrap nodes to the topology and connect to them.
	for bootnode in config.boot_nodes.iter() {
		match parse_str_addr(bootnode) {
			Ok((peer_id, addr)) => {
				Swarm::topology_mut(&mut swarm).add_bootstrap_addr(&peer_id, addr.clone());
				// TODO:
				/*if let Err(_) = Swarm::dial(&mut swarm, peer_id) {
					warn!(target: "sub-libp2p", "Failed to dial boot node: {}", bootnode);
				}*/
				Swarm::dial(&mut swarm, peer_id);
			},
			Err(_) => {
				// If the format of the bootstrap node is not a multiaddr, try to parse it as
				// a `SocketAddr`. This corresponds to the format `IP:PORT`.
				let addr = match bootnode.parse::<SocketAddr>() { 
					Ok(SocketAddr::V4(socket)) => multiaddr![Ip4(*socket.ip()), Tcp(socket.port())],
					Ok(SocketAddr::V6(socket)) => multiaddr![Ip6(*socket.ip()), Tcp(socket.port())],
					_ => {
						warn!(target: "sub-libp2p", "Not a valid bootnode address: {}", bootnode);
						continue;
					}
				};

				info!(target: "sub-libp2p", "Dialing {} with no peer id", addr);
				info!(target: "sub-libp2p", "Keep in mind that doing so is vulnerable to man-in-the-middle attacks");
				if let Err(addr) = Swarm::dial_addr(&mut swarm, addr) {
					warn!(target: "sub-libp2p", "Bootstrap address not supported: {}", addr);
				}
			},
		}
	}

	// Initialize the reserved peers.
	for reserved in config.reserved_nodes.iter() {
		match parse_str_addr(reserved) {
			Ok((peer_id, addr)) => {
				Swarm::topology_mut(&mut swarm).add_bootstrap_addr(&peer_id, addr.clone());
				swarm.add_reserved_peer(peer_id, addr.clone());
				// TODO:
				/*if let Err(_) = Swarm::dial(&mut swarm, peer_id) {
					warn!(target: "sub-libp2p", "Failed to dial reserved node: {}", reserved);
				}*/
				Swarm::dial(&mut swarm, peer_id);
			},
			Err(_) =>
				// TODO: also handle the `IP:PORT` format ; however we need to somehow add the
				// reserved ID to `reserved_peers` at some point
				warn!(target: "sub-libp2p", "Not a valid reserved node address: {}", reserved),
		}
	}

	debug!(target: "sub-libp2p", "Topology started with {} entries", Swarm::topology_mut(&mut swarm).num_peers());

	Ok(Service {
		swarm,
		nodes_addresses: Default::default(),
		index_by_id: Default::default(),
		next_connect_to_nodes: Delay::new(Instant::now()),
		next_kad_random_query: Interval::new(Instant::now() + Duration::from_secs(5), Duration::from_secs(45)),
		cleanup: Interval::new_interval(Duration::from_secs(60)),
		injected_events: Vec::new(),
	})
}

/// Event produced by the service.
#[derive(Debug)]
pub enum ServiceEvent {
	/// Closed connection to a node.
	///
	/// It is guaranteed that this node has been opened with a `NewNode` event beforehand. However
	/// not all `ClosedCustomProtocol` events have been dispatched.
	NodeClosed {
		/// Index of the node.
		node_index: NodeIndex,
		/// List of custom protocols that were still open.
		closed_custom_protocols: Vec<ProtocolId>,
	},

	/// A custom protocol substream has been opened with a node.
	OpenedCustomProtocol {
		/// Index of the node.
		node_index: NodeIndex,
		/// Protocol that has been opened.
		protocol: ProtocolId,
		/// Version of the protocol that was opened.
		version: u8,
	},

	/// A custom protocol substream has been closed.
	ClosedCustomProtocol {
		/// Index of the node.
		node_index: NodeIndex,
		/// Protocol that has been closed.
		protocol: ProtocolId,
	},

	/// Sustom protocol substreams has been closed.
	///
	/// Same as `ClosedCustomProtocol` but with multiple protocols.
	ClosedCustomProtocols {
		/// Index of the node.
		node_index: NodeIndex,
		/// Protocols that have been closed.
		protocols: Vec<ProtocolId>,
	},

	/// Receives a message on a custom protocol stream.
	CustomMessage {
		/// Index of the node.
		node_index: NodeIndex,
		/// Protocol which generated the message.
		protocol_id: ProtocolId,
		/// Data that has been received.
		data: Bytes,
	},
}

/// Network service. Must be polled regularly in order for the networking to work.
pub struct Service {
	/// Stream of events of the swarm.
	swarm: Swarm<Boxed<(PeerId, StreamMuxerBox)>, Behaviour<Substream<StreamMuxerBox>>, NetTopology>,

	/// For each node we're connected to, how we're connected to it.
	nodes_addresses: FnvHashMap<NodeIndex, (PeerId, ConnectedPoint)>,

	/// Opposite of `nodes_addresses`.
	index_by_id: FnvHashMap<PeerId, NodeIndex>,

	/// Future that will fire when we need to connect to new nodes.
	next_connect_to_nodes: Delay,

	/// Stream that fires when we need to perform the next Kademlia query.
	next_kad_random_query: Interval,

	/// Stream that fires when we need to cleanup and flush the topology, and cleanup the disabled
	/// peers.
	cleanup: Interval,

	/// Events to produce on the Stream.
	injected_events: Vec<ServiceEvent>,
}

impl Service {
	/// Returns an iterator that produces the list of addresses we're listening on.
	#[inline]
	pub fn listeners(&self) -> impl Iterator<Item = &Multiaddr> {
		Swarm::listeners(&self.swarm)
	}

	/// Returns the peer id of the local node.
	#[inline]
	pub fn peer_id(&self) -> &PeerId {
		unimplemented!()// TODO: self.kad_system.local_peer_id()
	}

	/// Returns the list of all the peers we are connected to.
	#[inline]
	pub fn connected_peers<'a>(&'a self) -> impl Iterator<Item = NodeIndex> + 'a {
		self.nodes_addresses.keys().cloned()
	}

	/// Try to add a reserved peer.
	pub fn add_reserved_peer(&mut self, peer_id: PeerId, addr: Multiaddr) {
		Swarm::topology_mut(&mut self.swarm).add_bootstrap_addr(&peer_id, addr.clone());
		self.swarm.add_reserved_peer(peer_id.clone(), addr.clone());
		Swarm::dial(&mut self.swarm, peer_id);
	}

	/// Try to remove a reserved peer.
	///
	/// If we are in reserved mode and we were connected to a node with this peer ID, then this
	/// method will disconnect it and return its index.
	pub fn remove_reserved_peer(&mut self, peer_id: PeerId) -> Option<NodeIndex> {
		self.swarm.remove_reserved_peer(peer_id);
		None		// TODO:
	}

	/// Start accepting all peers again if we weren't.
	#[inline]
	pub fn accept_unreserved_peers(&mut self) {
		self.swarm.accept_unreserved_peers();
	}

	/// Start refusing non-reserved nodes. Returns the list of nodes that have been disconnected.
	pub fn deny_unreserved_peers(&mut self) -> Vec<NodeIndex> {
		self.swarm.deny_unreserved_peers();
		unimplemented!()		// TODO:
	}

	/// Returns the `PeerId` of a node.
	#[inline]
	pub fn peer_id_of_node(&self, node_index: NodeIndex) -> Option<&PeerId> {
		self.nodes_addresses.get(&node_index).map(|(id, _)| id)
	}

	/// Returns the way we are connected to a node.
	#[inline]
	pub fn node_endpoint(&self, node_index: NodeIndex) -> Option<&ConnectedPoint> {
		self.nodes_addresses.get(&node_index).map(|(_, cp)| cp)
	}

	/// Sends a message to a peer using the custom protocol.
	// TODO: report invalid node index or protocol?
	pub fn send_custom_message(
		&mut self,
		node_index: NodeIndex,
		protocol: ProtocolId,
		data: Vec<u8>
	) {
		if let Some(peer_id) = self.peer_id_of_node(node_index) {
			self.swarm.send_custom_message(peer_id, protocol, data)
		}
	}

	/// Disconnects a peer and bans it for a little while.
	///
	/// Same as `drop_node`, except that the same peer will not be able to reconnect later.
	#[inline]
	pub fn ban_node(&mut self, node_index: NodeIndex) {
		if let Some(peer_id) = self.peer_id_of_node(node_index) {
			info!(target: "sub-libp2p", "Banned {:?}", peer_id);
			self.swarm.ban_node(peer_id.clone());
		}
	}

	/// Disconnects a peer.
	///
	/// This is asynchronous and will not immediately close the peer.
	/// Corresponding closing events will be generated once the closing actually happens.
	#[inline]
	pub fn drop_node(&mut self, node_index: NodeIndex) {
		if let Some(peer_id) = self.peer_id_of_node(node_index) {
			self.swarm.drop_node(peer_id);
		}
	}

	/*/// Counts the number of non-reserved ingoing connections.
	fn num_ingoing_connections(&self) -> usize {
		self.swarm.nodes()
			.filter(|&i| self.swarm.node_endpoint(i) == Some(Endpoint::Listener) &&
				!self.reserved_peers.contains(&self.swarm.peer_id_of_node(i).unwrap()))
			.count()
	}

	/// Counts the number of non-reserved outgoing connections.
	fn num_outgoing_connections(&self) -> usize {
		self.swarm.nodes()
			.filter(|&i| self.swarm.node_endpoint(i) == Some(Endpoint::Dialer) &&
				!self.reserved_peers.contains(&self.swarm.peer_id_of_node(i).unwrap()))
			.count()
	}

	/// Updates the attempted connections to nodes.
	///
	/// Also updates `next_connect_to_nodes` with the earliest known moment when we need to
	/// update connections again.
	fn connect_to_nodes(&mut self) {
		// Make sure we are connected or connecting to all the reserved nodes.
		for reserved in self.reserved_peers.iter() {
			let addrs = Swarm::topology_mut(&mut self.swarm).addrs_of_peer(&reserved);
			for (addr, _) in addrs {
				let _ = self.swarm.ensure_connection(reserved.clone(), addr.clone());
			}
		}

		// Counter of number of connections to open, decreased when we open one.
		let mut num_to_open = self.max_outgoing_connections - self.num_outgoing_connections();

		let (to_try, will_change) = Swarm::topology_mut(&mut self.swarm).addrs_to_attempt();
		for (peer_id, addr) in to_try {
			if num_to_open == 0 {
				break;
			}

			if peer_id == self.kad_system.local_peer_id() {
				continue;
			}

			if self.disabled_peers.contains_key(&peer_id) {
				continue;
			}

			// It is possible that we are connected to this peer, but the topology doesn't know
			// about that because it is an incoming connection.
			match self.swarm.ensure_connection(peer_id.clone(), addr.clone()) {
				Ok(true) => (),
				Ok(false) => num_to_open -= 1,
				Err(_) => ()
			}
		}

		self.next_connect_to_nodes.reset(will_change);
	}

	/// Handles the swarm opening a connection to the given peer.
	///
	/// > **Note**: Must be called from inside `poll()`, otherwise it will panic.
	fn handle_connection(
		&mut self,
		node_index: NodeIndex,
		peer_id: PeerId,
		endpoint: ConnectedPoint
	) {
		// Reject connections to our own node, which can happen if the DHT contains `127.0.0.1`
		// for example.
		if &peer_id == self.kad_system.local_peer_id() {
			debug!(target: "sub-libp2p", "Rejected connection to/from ourself: {:?}", endpoint);
			assert_eq!(self.swarm.drop_node(node_index), Ok(Vec::new()));
			if let ConnectedPoint::Dialer { ref address } = endpoint {
				Swarm::topology_mut(&mut self.swarm).report_failed_to_connect(address);
			}
			return;
		}

		// Reject non-reserved nodes if we're in reserved mode.
		let is_reserved = self.reserved_peers.contains(&peer_id);
		if self.reserved_only && !is_reserved {
			debug!(target: "sub-libp2p", "Rejected non-reserved peer {:?}", peer_id);
			assert_eq!(self.swarm.drop_node(node_index), Ok(Vec::new()));
			if let ConnectedPoint::Dialer { ref address } = endpoint {
				Swarm::topology_mut(&mut self.swarm).report_failed_to_connect(address);
			}
			return;
		}

		// Reject connections from disabled peers.
		if let Some(expires) = self.disabled_peers.get(&peer_id) {
			if expires > &Instant::now() {
				info!(target: "sub-libp2p", "Rejected connection from disabled peer: {:?}", peer_id);
				assert_eq!(self.swarm.drop_node(node_index), Ok(Vec::new()));
				if let ConnectedPoint::Dialer { ref address } = endpoint {
					Swarm::topology_mut(&mut self.swarm).report_failed_to_connect(address);
				}
				return;
			}
		}

		match endpoint {
			ConnectedPoint::Listener { ref listen_addr, ref send_back_addr } => {
				if is_reserved || self.num_ingoing_connections() < self.max_incoming_connections {
					debug!(target: "sub-libp2p", "Connected to {:?} through {} on listener {}",
						peer_id, send_back_addr, listen_addr);
				} else {
					info!(target: "sub-libp2p", "Rejected incoming peer {:?} because we are full", peer_id);
					assert_eq!(self.swarm.drop_node(node_index), Ok(Vec::new()));
					return;
				}
			},
			ConnectedPoint::Dialer { ref address } => {
				if is_reserved || self.num_outgoing_connections() < self.max_outgoing_connections {
					debug!(target: "sub-libp2p", "Connected to {:?} through {}", peer_id, address);
					Swarm::topology_mut(&mut self.swarm).report_connected(address, &peer_id);
				} else {
					debug!(target: "sub-libp2p", "Rejected dialed peer {:?} because we are full", peer_id);
					assert_eq!(self.swarm.drop_node(node_index), Ok(Vec::new()));
					return;
				}
			},
		};

		if let Err(_) = self.swarm.accept_node(node_index) {
			error!(target: "sub-libp2p", "accept_node returned an error");
		}

		// We are finally sure that we're connected.

		if let ConnectedPoint::Dialer { ref address } = endpoint {
			Swarm::topology_mut(&mut self.swarm).report_connected(address, &peer_id);
		}
		self.nodes_addresses.insert(node_index, endpoint.clone());
	}*/

	/*/// Processes an event generated by the swarm.
	///
	/// Optionally returns an event to report back to the outside.
	///
	/// > **Note**: Must be called from inside `poll()`, otherwise it will panic.
	fn process_network_event(
		&mut self,
		event: SwarmEvent
	) -> Option<ServiceEvent> {
		match event {
			SwarmEvent::NodePending { node_index, peer_id, endpoint } => {
				self.handle_connection(node_index, peer_id, endpoint);
				None
			},
			SwarmEvent::Reconnected { node_index, endpoint, closed_custom_protocols } => {
				if let Some(ConnectedPoint::Dialer { address }) = self.nodes_addresses.remove(&node_index) {
					Swarm::topology_mut(&mut self.swarm).report_disconnected(&address, DisconnectReason::FoundBetterAddr);
				}
				if let ConnectedPoint::Dialer { ref address } = endpoint {
					let peer_id = self.swarm.peer_id_of_node(node_index)
						.expect("the swarm always produces events containing valid node indices");
					Swarm::topology_mut(&mut self.swarm).report_connected(address, peer_id);
				}
				self.nodes_addresses.insert(node_index, endpoint);
				Some(ServiceEvent::ClosedCustomProtocols {
					node_index,
					protocols: closed_custom_protocols,
				})
			},
			SwarmEvent::NodeClosed { node_index, peer_id, closed_custom_protocols } => {
				debug!(target: "sub-libp2p", "Connection to {:?} closed gracefully", peer_id);
				if let Some(ConnectedPoint::Dialer { ref address }) = self.nodes_addresses.get(&node_index) {
					Swarm::topology_mut(&mut self.swarm).report_disconnected(address, DisconnectReason::RemoteClosed);
				}
				self.connect_to_nodes();
				Some(ServiceEvent::NodeClosed {
					node_index,
					closed_custom_protocols,
				})
			},
			SwarmEvent::DialFail { address, error } => {
				debug!(target: "sub-libp2p", "Failed to dial address {}: {:?}", address, error);
				Swarm::topology_mut(&mut self.swarm).report_failed_to_connect(&address);
				self.connect_to_nodes();
				None
			},
			SwarmEvent::UnresponsiveNode { node_index } => {
				let closed_custom_protocols = self.swarm.drop_node(node_index)
					.expect("the swarm always produces events containing valid node indices");
				if let Some(ConnectedPoint::Dialer { address }) = self.nodes_addresses.remove(&node_index) {
					Swarm::topology_mut(&mut self.swarm).report_disconnected(&address, DisconnectReason::Useless);
				}
				Some(ServiceEvent::NodeClosed {
					node_index,
					closed_custom_protocols,
				})
			},
			SwarmEvent::UselessNode { node_index } => {
				let peer_id = self.swarm.peer_id_of_node(node_index)
					.expect("the swarm always produces events containing valid node indices")
					.clone();
				let closed_custom_protocols = self.swarm.drop_node(node_index)
					.expect("the swarm always produces events containing valid node indices");
				Swarm::topology_mut(&mut self.swarm).report_useless(&peer_id);
				if let Some(ConnectedPoint::Dialer { address }) = self.nodes_addresses.remove(&node_index) {
					Swarm::topology_mut(&mut self.swarm).report_disconnected(&address, DisconnectReason::Useless);
				}
				Some(ServiceEvent::NodeClosed {
					node_index,
					closed_custom_protocols,
				})
			},
			SwarmEvent::NodeInfos { node_index, listen_addrs, .. } => {
				let peer_id = self.swarm.peer_id_of_node(node_index)
					.expect("the swarm always produces events containing valid node indices");
				Swarm::topology_mut(&mut self.swarm).add_self_reported_listen_addrs(
					peer_id,
					listen_addrs.into_iter()
				);
				None
			},
			SwarmEvent::KadFindNode { searched, responder, .. } => {
				let response = self.build_kademlia_response(&searched);
				responder.respond(response);
				None
			},
			SwarmEvent::KadOpen { node_index, controller } => {
				let peer_id = self.swarm.peer_id_of_node(node_index)
					.expect("the swarm always produces events containing valid node indices");
				trace!(target: "sub-libp2p", "Opened Kademlia substream with {:?}", peer_id);
				if let Some(list) = self.kad_pending_ctrls.lock().remove(&peer_id) {
					for tx in list {
						let _ = tx.send(controller.clone());
					}
				}
				None
			},
			SwarmEvent::KadClosed { .. } => {
				None
			},
			SwarmEvent::OpenedCustomProtocol { node_index, protocol, version } => {
				let peer_id = self.swarm.peer_id_of_node(node_index)
					.expect("the swarm always produces events containing valid node indices");
				self.kad_system.update_kbuckets(peer_id.clone());
				Some(ServiceEvent::OpenedCustomProtocol {
					node_index,
					protocol,
					version,
				})
			},
			SwarmEvent::ClosedCustomProtocol { node_index, protocol } =>
				Some(ServiceEvent::ClosedCustomProtocol {
					node_index,
					protocol,
				}),
			SwarmEvent::CustomMessage { node_index, protocol_id, data } => {
				let peer_id = self.swarm.peer_id_of_node(node_index)
					.expect("the swarm always produces events containing valid node indices");
				self.kad_system.update_kbuckets(peer_id.clone());
				Some(ServiceEvent::CustomMessage {
					node_index,
					protocol_id,
					data,
				})
			},
		}
	}*/

	/// Polls for what happened on the main network side.
	fn poll_swarm(&mut self) -> Poll<Option<ServiceEvent>, IoError> {
		loop {
			match self.swarm.poll() {
				Ok(Async::Ready(Some(event))) => (),
					// TODO:
					/*if let Some(event) = self.process_network_event(event) {
						return Ok(Async::Ready(Some(event)));
					}*/
				Ok(Async::NotReady) => return Ok(Async::NotReady),
				Ok(Async::Ready(None)) => unreachable!("The Swarm stream never ends"),
				// TODO: this `Err` contains a `Void` ; remove variant when Rust allows that
				Err(_) => unreachable!("The Swarm stream never errors"),
			}
		}
	}

	/// Polls the Kademlia-related events.
	fn poll_kademlia(&mut self) -> Poll<Option<ServiceEvent>, IoError> {
		// Poll the future that fires when we need to perform a random Kademlia query.
		loop {
			match self.next_kad_random_query.poll() {
				Ok(Async::NotReady) => break,
				Ok(Async::Ready(Some(_))) => {
					self.swarm.perform_kad_random_query();
				},
				Ok(Async::Ready(None)) => {
					warn!(target: "sub-libp2p", "Kad query timer closed unexpectedly");
					return Ok(Async::Ready(None));
				}
				Err(err) => {
					warn!(target: "sub-libp2p", "Kad query timer errored: {:?}", err);
					return Err(IoError::new(IoErrorKind::Other, err));
				}
			}
		}

		Ok(Async::NotReady)
	}

	// Polls the future that fires when we need to refresh our connections.
	fn poll_next_connect_refresh(&mut self) -> Poll<Option<ServiceEvent>, IoError> {
		loop {
			match self.next_connect_to_nodes.poll() {
				Ok(Async::Ready(())) => {},// TODO: self.connect_to_nodes(),
				Ok(Async::NotReady) => return Ok(Async::NotReady),
				Err(err) => {
					warn!(target: "sub-libp2p", "Connect to nodes timer errored: {:?}", err);
					return Err(IoError::new(IoErrorKind::Other, err));
				}
			}
		}
	}

	/// Polls the stream that fires when we need to cleanup and flush the topology.
	fn poll_cleanup(&mut self) -> Poll<Option<ServiceEvent>, IoError> {
		loop {
			match self.cleanup.poll() {
				Ok(Async::NotReady) => return Ok(Async::NotReady),
				Ok(Async::Ready(Some(_))) => {
					debug!(target: "sub-libp2p", "Cleaning and flushing topology");
					Swarm::topology_mut(&mut self.swarm).cleanup();
					if let Err(err) = Swarm::topology_mut(&mut self.swarm).flush_to_disk() {
						warn!(target: "sub-libp2p", "Failed to flush topology: {:?}", err);
					}
					debug!(target: "sub-libp2p", "Topology now contains {} nodes",
						Swarm::topology_mut(&mut self.swarm).num_peers());
				},
				Ok(Async::Ready(None)) => {
					warn!(target: "sub-libp2p", "Topology flush stream ended unexpectedly");
					return Ok(Async::Ready(None));
				}
				Err(err) => {
					warn!(target: "sub-libp2p", "Topology flush stream errored: {:?}", err);
					return Err(IoError::new(IoErrorKind::Other, err));
				}
			}
		}
	}
}

impl Drop for Service {
	fn drop(&mut self) {
		if let Err(err) = Swarm::topology_mut(&mut self.swarm).flush_to_disk() {
			warn!(target: "sub-libp2p", "Failed to flush topology: {:?}", err);
		}
	}
}

impl Stream for Service {
	type Item = ServiceEvent;
	type Error = IoError;

	fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
		if !self.injected_events.is_empty() {
			return Ok(Async::Ready(Some(self.injected_events.remove(0))));
		}

		match self.poll_swarm()? {
			Async::Ready(value) => return Ok(Async::Ready(value)),
			Async::NotReady => (),
		}

		match self.poll_kademlia()? {
			Async::Ready(value) => return Ok(Async::Ready(value)),
			Async::NotReady => (),
		}

		match self.poll_next_connect_refresh()? {
			Async::Ready(value) => return Ok(Async::Ready(value)),
			Async::NotReady => (),
		}

		match self.poll_cleanup()? {
			Async::Ready(value) => return Ok(Async::Ready(value)),
			Async::NotReady => (),
		}

		// The only way we reach this is if we went through all the `NotReady` paths above,
		// ensuring the current task is registered everywhere.
		Ok(Async::NotReady)
	}
}
