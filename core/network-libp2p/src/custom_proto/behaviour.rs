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
	registered_protocols: RegisteredProtocols,

	/// List of protocols that we have open.
	open_protocols: Vec<(PeerId, ProtocolId)>,

	/// Events to produce from `poll()`.
	events: SmallVec<[NetworkBehaviourAction<CustomProtosHandlerIn, (PeerId, CustomProtosHandlerOut)>; 4]>,

	/// Marker to pin the generics.
	marker: PhantomData<TSubstream>,
}

impl<TSubstream> CustomProtosBehaviour<TSubstream> {
	/// Creates a `CustomProtosBehaviour`.
	pub fn new(registered_protocols: RegisteredProtocols) -> Self {
		CustomProtosBehaviour {
			registered_protocols,
			open_protocols: Vec::with_capacity(50),		// TODO: pass capacity to constructor
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
		CustomProtosHandler::new(self.registered_protocols.clone())
	}

	fn inject_connected(&mut self, _: PeerId, _: ConnectedPoint) {}

	fn inject_disconnected(&mut self, peer_id: &PeerId, _: ConnectedPoint) {
		if let Some(pos) = self.open_protocols.iter().position(|(p, _)| p == peer_id) {
			let (_, protocol_id) = self.open_protocols.remove(pos);
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
				let pos = self.open_protocols.iter().position(|(s, p)| {
					s == &source && p == protocol_id
				});

				if let Some(pos) = pos {
					self.open_protocols.remove(pos);
				} else {
					debug_assert!(false, "Couldn't find protocol in open_protocols");
				}
			},
			CustomProtosHandlerOut::CustomProtocolOpen { ref protocol_id, .. } => {
				debug_assert!(!self.open_protocols.iter().any(|(s, p)| {
					s == &source && p == protocol_id
				}));
				self.open_protocols.push((source.clone(), protocol_id.clone()));
			},
		}

		self.events.push(NetworkBehaviourAction::GenerateEvent((source, event)));
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
