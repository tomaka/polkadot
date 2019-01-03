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

use crate::ProtocolId;
use crate::custom_proto::upgrade::{RegisteredProtocol, RegisteredProtocols, RegisteredProtocolSubstream};
use bytes::Bytes;
use futures::prelude::*;
use libp2p::core::{
	Endpoint, ProtocolsHandler, ProtocolsHandlerEvent,
	protocols_handler::ProtocolsHandlerUpgrErr,
	upgrade::{InboundUpgrade, OutboundUpgrade}
};
use smallvec::SmallVec;
use std::{fmt, io};
use tokio_io::{AsyncRead, AsyncWrite};
use void::Void;

/// Protocol handler that tries to maintain one substream per registered custom protocol.
pub struct CustomProtosHandler<TSubstream> {
	/// List of all the protocols we support.
	protocols: RegisteredProtocols,

	/// If true, we are trying to shut down the existing node and thus should refuse any incoming
	/// connection.
	shutting_down: bool,

	/// The active substreams. There should always ever be only one substream per protocol.
	substreams: SmallVec<[RegisteredProtocolSubstream<TSubstream>; 6]>,

	/// Queue of events to send to the outside.
	events_queue: SmallVec<[ProtocolsHandlerEvent<RegisteredProtocol, (), CustomProtosHandlerOut>; 16]>,
}

/// Event that can be received by a `CustomProtosHandler`.
#[derive(Debug)]
pub enum CustomProtosHandlerIn {
	/// Sends a message through a custom protocol substream.
	SendCustomMessage {
		/// The protocol to use.
		protocol: ProtocolId,
		/// The data to send.
		data: Bytes,
	},
}

/// Event that can be emitted by a `CustomProtosHandler`.
#[derive(Debug)]
pub enum CustomProtosHandlerOut {
	/// Opened a custom protocol with the remote.
	CustomProtocolOpen {
		/// Identifier of the protocol.
		protocol_id: ProtocolId,
		/// Version of the protocol that has been opened.
		version: u8,
		/// Whether the substream was inbound or outbound.
		endpoint: Endpoint,
	},

	/// Closed a custom protocol with the remote.
	CustomProtocolClosed {
		/// Identifier of the protocol.
		protocol_id: ProtocolId,
		/// Reason why the substream closed. If `Ok`, then it's a graceful exit (EOF).
		result: io::Result<()>,
	},

	/// Receives a message on a custom protocol substream.
	CustomMessage {
		/// Protocol which generated the message.
		protocol_id: ProtocolId,
		/// Data that has been received.
		data: Bytes,
	},

	/// Signals that the peer is useless.
	UselessNode,
}

impl<TSubstream> CustomProtosHandler<TSubstream>
where
	TSubstream: AsyncRead + AsyncWrite,
{
	/// Builds a new `CustomProtosHandler`.
	pub fn new(protocols: RegisteredProtocols) -> Self {
		// Try open one substream for each registered protocol.
		let mut events_queue = SmallVec::new();
		for protocol in protocols.0.iter() {
			events_queue.push(ProtocolsHandlerEvent::OutboundSubstreamRequest {
				upgrade: protocol.clone(),
				info: (),
			});
		}

		CustomProtosHandler {
			protocols,
			shutting_down: false,
			substreams: SmallVec::new(),
			events_queue,
		}
	}

	/// Called by `inject_fully_negotiated_inbound` and `inject_fully_negotiated_outbound`.
	fn inject_fully_negotiated(
		&mut self,
		proto: RegisteredProtocolSubstream<TSubstream>,
		endpoint: Endpoint,
	) {
		if self.shutting_down {
			return;
		}

		if self.substreams.iter().any(|p| p.protocol_id() == proto.protocol_id()) {
			// Skipping protocol that's already open.
			return;
		}

		let event = CustomProtosHandlerOut::CustomProtocolOpen {
			protocol_id: proto.protocol_id(),
			version: proto.protocol_version(),
			endpoint,
		};

		self.substreams.push(proto);
		self.events_queue.push(ProtocolsHandlerEvent::Custom(event));
	}
}

impl<TSubstream> ProtocolsHandler for CustomProtosHandler<TSubstream>
where
	TSubstream: AsyncRead + AsyncWrite,
{
	type InEvent = CustomProtosHandlerIn;
	type OutEvent = CustomProtosHandlerOut;
	type Substream = TSubstream;
	type Error = Void;
	type InboundProtocol = RegisteredProtocols;
	type OutboundProtocol = RegisteredProtocol;
	type OutboundOpenInfo = ();

	#[inline]
	fn listen_protocol(&self) -> Self::InboundProtocol {
		self.protocols.clone()
	}

	fn inject_fully_negotiated_inbound(
		&mut self,
		proto: <Self::InboundProtocol as InboundUpgrade<TSubstream>>::Output
	) {
		self.inject_fully_negotiated(proto, Endpoint::Listener);
	}

	#[inline]
	fn inject_fully_negotiated_outbound(
		&mut self,
		proto: <Self::OutboundProtocol as OutboundUpgrade<TSubstream>>::Output,
		_: Self::OutboundOpenInfo
	) {
		self.inject_fully_negotiated(proto, Endpoint::Dialer);
	}

	fn inject_event(&mut self, message: CustomProtosHandlerIn) {
		match message {
			CustomProtosHandlerIn::SendCustomMessage { protocol, data } => {
				debug_assert!(self.protocols.has_protocol(protocol),
					"invalid protocol id requested in the API of the libp2p networking");
				let proto = match self.substreams.iter_mut().find(|p| p.protocol_id() == protocol) {
					Some(proto) => proto,
					None => {
						// We are processing a message event before we could report to the outside
						// that we disconnected from the protocol. This is not an error.
						trace!(target: "sub-libp2p", "Tried to send message through closed \
							protocol");
						return
					},
				};

				proto.send_message(data);
			},
		}
	}

	#[inline]
	fn inject_inbound_closed(&mut self) {}

	#[inline]
	fn inject_dial_upgrade_error(&mut self, _: Self::OutboundOpenInfo, _: ProtocolsHandlerUpgrErr<io::Error>) {
		// Right now if the remote doesn't support one of the custom protocols, we shut down the
		// entire connection. This is a hack-ish solution to the problem where we connect to nodes
		// that support libp2p but not the testnet that we want.
		self.events_queue.push(ProtocolsHandlerEvent::Custom(CustomProtosHandlerOut::UselessNode));
		self.shutdown();
	}

	fn shutdown(&mut self) {
		self.shutting_down = true;
		for substream in self.substreams.iter_mut() {
			substream.shutdown();
		}
	}

	fn poll(
		&mut self,
	) -> Poll<
		ProtocolsHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::OutEvent>,
		Self::Error,
	> {
		if !self.events_queue.is_empty() {
			let event = self.events_queue.remove(0);
			return Ok(Async::Ready(event));
		}

		if self.shutting_down && self.substreams.is_empty() {
			return Ok(Async::Ready(ProtocolsHandlerEvent::Shutdown));
		}

		for n in (0..self.substreams.len()).rev() {
			let mut substream = self.substreams.swap_remove(n);
			loop {
				match substream.poll() {
					Ok(Async::Ready(Some(data))) => {
						let event = CustomProtosHandlerOut::CustomMessage {
							protocol_id: substream.protocol_id(),
							data
						};
						self.substreams.push(substream);
						return Ok(Async::Ready(ProtocolsHandlerEvent::Custom(event)));
					},
					Ok(Async::NotReady) => {
						self.substreams.push(substream);
						break;
					},
					Ok(Async::Ready(None)) => {
						let event = CustomProtosHandlerOut::CustomProtocolClosed {
							protocol_id: substream.protocol_id(),
							result: Ok(())
						};
						return Ok(Async::Ready(ProtocolsHandlerEvent::Custom(event)));
					},
					Err(err) => {
						let event = CustomProtosHandlerOut::CustomProtocolClosed {
							protocol_id: substream.protocol_id(),
							result: Err(err)
						};
						return Ok(Async::Ready(ProtocolsHandlerEvent::Custom(event)));
					},
				}
			}
		}

		Ok(Async::NotReady)
	}
}

impl<TSubstream> fmt::Debug for CustomProtosHandler<TSubstream>
where
	TSubstream: AsyncRead + AsyncWrite,
{
	fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
		f.debug_struct("CustomProtosHandler")
			.field("protocols", &self.protocols.len())
			.field("shutting_down", &self.shutting_down)
			.field("substreams", &self.substreams.len())
			.finish()
	}
}
