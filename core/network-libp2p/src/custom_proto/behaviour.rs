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
use crate::ProtocolId;
use bytes::Bytes;
use futures::prelude::*;
use libp2p::core::nodes::{ConnectedPoint, NetworkBehaviour, NetworkBehaviourAction};
use libp2p::core::{protocols_handler::ProtocolsHandler, PeerId};
use smallvec::SmallVec;
use std::marker::PhantomData;
use tokio_io::{AsyncRead, AsyncWrite};

/// Network behaviour that handles opening substreams for custom protocols with other nodes.
pub struct CustomProtosBehaviour<TSubstream> {
	/// List of protocols to open with peers. Never modified.
	protocols: RegisteredProtocols,

	/// Events to produce from `poll()`.
	events: SmallVec<[NetworkBehaviourAction<CustomProtosHandlerIn, (PeerId, CustomProtosHandlerOut)>; 4]>,

	/// Marker to pin the generics.
	marker: PhantomData<TSubstream>,
}

impl<TSubstream> CustomProtosBehaviour<TSubstream> {
	/// Creates a `CustomProtosBehaviour`.
	pub fn new(protocols: RegisteredProtocols) -> Self {
		CustomProtosBehaviour {
			protocols,
			events: SmallVec::new(),
			marker: PhantomData,
		}
	}
}

impl<TSubstream> CustomProtosBehaviour<TSubstream>
where
	TSubstream: AsyncRead + AsyncWrite,
{
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
}

impl<TSubstream, TTopology> NetworkBehaviour<TTopology> for CustomProtosBehaviour<TSubstream>
where
	TSubstream: AsyncRead + AsyncWrite,
{
	type ProtocolsHandler = CustomProtosHandler<TSubstream>;
	type OutEvent = (PeerId, CustomProtosHandlerOut);

	fn new_handler(&mut self) -> Self::ProtocolsHandler {
		CustomProtosHandler::new(self.protocols.clone())
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
		_: &mut TTopology,
	) -> Async<
		NetworkBehaviourAction<
			<Self::ProtocolsHandler as ProtocolsHandler>::InEvent,
			Self::OutEvent,
		>,
	> {
		if !self.events.is_empty() {
			Async::Ready(self.events.remove(0))
		} else {
			Async::NotReady
		}
	}
}
