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

use bytes::Bytes;
use custom_handler::{CustomProtoHandler, CustomProtoHandlerEvent};
use custom_proto::{RegisteredProtocols, RegisteredProtocolSubstream};
use futures::{prelude::*, task};
use libp2p::core::{ConnectionUpgrade, Endpoint, PeerId, PublicKey, upgrade, nodes::MapOutEvent};
use libp2p::{identify, kad, ping};
use std::io;
use std::time::{Duration, Instant};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_timer::{Delay, Interval};
use {Multiaddr, ProtocolId};

/// Implementation of `ProtocolsHandler` for our node.
#[derive(Debug, ProtocolsHandler, Default)]
pub struct SubstrateProtocolsHandler<TSubstream> {
	/// Handles the custom Substrate-specific protocols.
	custom: MapOutEvent<CustomProtoHandler<TSubstream>, fn(CustomProtoHandlerEvent) -> SubstrateOutEvent<TSubstream>>,
	/// Handles pinging the remote nodes at a regular interval.
	ping: MapOutEvent<ping::PeriodicPingHandler<TSubstream>, fn(ping::AutoDcBehaviourEvent) -> SubstrateOutEvent<TSubstream>>,
	/// Handles Kademlia querying and answering.
	kad: MapOutEvent<kad::KademliaHandler<TSubstream>, fn(kad::KademliaHandlerEvent) -> SubstrateOutEvent<TSubstream>>,
	/// Handles periodically identifying remotes.
	identify: MapOutEvent<identify::PeriodicIdentification<TSubstream>, fn(identify::PeriodicIdentificationEvent) -> SubstrateOutEvent<TSubstream>>,
}

impl<TSubstream> SubstrateProtocolsHandler<TSubstream> {
	/// Builds a new protocols handler.
	pub fn new() -> Self {
		let custom = CustomProtoHandler::new()
			.map_out_event::<fn(_) -> _, _>(From::from);
		let ping = ping::PeriodicPingHandler::new()
			.map_out_event::<fn(_) -> _, _>(From::from);
		let kad = kad::KademliaHandler::new()
			.map_out_event::<fn(_) -> _, _>(From::from);
		let identify = identify::PeriodicIdentification::new()
			.map_out_event::<fn(_) -> _, _>(From::from);

		SubstrateProtocolsHandler {
			custom,
			ping,
			kad,
			identify,
		}
	}
}

/// Event that can happen on the `SubstrateNodeHandler`.
pub enum SubstrateOutEvent<TSubstream> {
	/// The node has been determined to be unresponsive.
	Unresponsive,

	/// The node works but we can't do anything useful with it.
	Useless,

	/// Started pinging the remote. This can be used to print a diagnostic message in the logs.
	PingStart,

	/// The node has successfully responded to a ping.
	PingSuccess(Duration),

	/// Opened a custom protocol with the remote.
	CustomProtocolOpen {
		/// Identifier of the protocol.
		protocol_id: ProtocolId,
		/// Version of the protocol that has been opened.
		version: u8,
	},

	/// Closed a custom protocol with the remote.
	CustomProtocolClosed {
		/// Identifier of the protocol.
		protocol_id: ProtocolId,
		/// Reason why the substream closed. If `Ok`, then it's a graceful exit (EOF).
		result: Result<(), io::Error>,
	},

	/// Receives a message on a custom protocol substream.
	CustomMessage {
		/// Protocol which generated the message.
		protocol_id: ProtocolId,
		/// Data that has been received.
		data: Bytes,
	},

	/// We obtained identification information from the remote
	Identified {
		/// Information of the remote.
		info: identify::IdentifyInfo,
		/// Address the remote observes us as.
		observed_addr: Multiaddr,
	},

	/// The remote wants us to answer a Kademlia `FIND_NODE` request.
	///
	/// The `responder` should be used to answer that query.
	// TODO: this API with the "responder" is bad, but changing it requires modifications in libp2p
	KadFindNode {
		/// The value being searched.
		searched: PeerId,
		/// Object to use to respond to the request.
		responder: KadFindNodeRespond,
	},

	/// The Kademlia substream has been closed.
	///
	/// The parameter contains the reason why it has been closed. `Ok` means that it's been closed
	/// gracefully.
	KadClosed(Result<(), io::Error>),

	/// An error happened while upgrading a substream.
	///
	/// This can be used to print a diagnostic message.
	SubstreamUpgradeFail(io::Error),
}

impl<TSubstream> From<CustomProtoHandlerEvent> for SubstrateOutEvent<TSubstream> {
	fn from(ev: CustomProtoHandlerEvent) -> Self {
		match ev {
			CustomProtoHandlerEvent::OpenedCustomProtocol { protocol, version } => {
				SubstrateOutEvent::OpenedCustomProtocol { protocol, version }
			},
			CustomProtoHandlerEvent::ClosedCustomProtocol { protocol } => {
				SubstrateOutEvent::ClosedCustomProtocol { protocol }
			},
			CustomProtoHandlerEvent::CustomMessage { protocol_id, data } => {
				SubstrateOutEvent::CustomMessage { protocol_id, data }
			},
			CustomProtoHandlerEvent::UselessNode => {
				SubstrateOutEvent::UselessNode
			},
		}
	}
}

impl<TSubstream> From<ping::AutoDcBehaviourEvent> for SubstrateOutEvent<TSubstream> {
	fn from(ev: ping::AutoDcBehaviourEvent) -> Self {
		match ev {
			ping::AutoDcBehaviourEvent::PingSuccess { peer_id, ping_time } => {
				SubstrateProtocolsHandler::PingSuccess { peer_id, ping_time }
			},
		}
	}
}

impl<TSubstream> From<kad::KademliaHandlerEvent> for SubstrateOutEvent<TSubstream> {
	#[inline]
	fn from(ev: kad::KademliaHandlerEvent) -> Self {
		/*match ev {
			kad::KademliaHandlerEvent::Open => {

			},
			kad::KademliaHandlerEvent::IgnoredIncoming => {

			},
			kad::KademliaHandlerEvent::Closed(result) => {
				
			},
			kad::KademliaHandlerEvent::FindNodeReq { key } => {

			}
			kad::KademliaHandlerEvent::FindNodeRes { closer_peers } => {
			},

			kad::KademliaHandlerEvent::GetProvidersReq { key } => {
			},

			kad::KademliaHandlerEvent::GetProvidersRes { closer_peers, provider_peers } => {
			},

			kad::KademliaHandlerEvent::AddProvider { key, provider_peer } => {
			},
		}*/
		unimplemented!()	// TODO:
	}
}

impl<TSubstream> From<identify::PeriodicIdentificationEvent> for SubstrateOutEvent<TSubstream> {
	fn from(ev: identify::PeriodicIdentificationEvent) -> Self {
		match ev {
			identify::PeriodicIdentificationEvent::Identified { info, observed_addr } => {
				SubstrateProtocolsHandler::Identified { info, observed_addr }
			},
		}
	}
}
