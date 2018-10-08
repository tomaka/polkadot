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
use custom_proto::{RegisteredProtocol, RegisteredProtocols, RegisteredProtocolSubstream};
use libp2p::core::{Multiaddr, ConnectionUpgrade, Endpoint};
use libp2p::core::nodes::{NodeHandlerEvent, NodeHandlerEndpoint, ProtocolsHandler};
use libp2p::tokio_codec::Framed;
use std::{collections::VecDeque, io, mem, vec::IntoIter as VecIntoIter};
use futures::{prelude::*, future, stream, task};
use tokio_io::{AsyncRead, AsyncWrite};
use unsigned_varint::codec::UviBytes;
use ProtocolId;

/// Protocol handler that handles opening custom protocols with the remotes.
pub struct CustomProtoHandler<TSubstream>
where TSubstream: AsyncRead + AsyncWrite,
{
	/// The registered protocols, and the substream associated to it.
	substreams: Vec<(RegisteredProtocol, SubstreamState<TSubstream>)>,

	/// Queue of events to produce when polling.
	pending_events: VecDeque<CustomProtoHandlerEvent>,
}

/// State of a substream.
enum SubstreamState<TSubstream> {
	/// Haven't open a substream to this protocol yet.
	NotOpen,
	/// We opened an outgoing substream for this protocol.
	Dialed,
	/// The substream is open and healthy.
	Open(RegisteredProtocolSubstream<TSubstream>),
	/// The substream has been closed on purpose.
	Closed,
	/// Something bad happened.
	Errored,
}

impl<TSubstream> SubstreamState<TSubstream> {
	/// Returns true for `Errored`.
	#[inline]
	pub fn is_error(&self) -> bool {
		match self {
			SubstreamState::Errored => true,
			SubstreamState::NotOpen => false,
			SubstreamState::Dialed => false,
			SubstreamState::Closed => false,
			SubstreamState::Open(_) => false,
		}
	}
}

/// Event produced by the handler.
#[derive(Debug)]
pub enum CustomProtoHandlerEvent {
	/// A custom protocol substream has been opened with a node.
	OpenedCustomProtocol {
		/// Protocol that has been opened.
		protocol: ProtocolId,
		/// Version of the protocol that was opened.
		version: u8,
	},

	/// A custom protocol substream has been closed.
	ClosedCustomProtocol {
		/// Protocol that has been closed.
		protocol: ProtocolId,
	},

	/// Receives a message on a custom protocol stream.
	CustomMessage {
		/// Protocol which generated the message.
		protocol_id: ProtocolId,
		/// Data that has been received.
		data: Bytes,
	},

	/// The node works but doesn't support any of our protocols.
	UselessNode,
}

impl<TSubstream> CustomProtoHandler<TSubstream>
where TSubstream: AsyncRead + AsyncWrite,
{
	/// Create a new `CustomProtoHandler`.
	pub fn new<TIter>(protocols: TIter) -> CustomProtoHandler<TSubstream>
	where TIter: IntoIterator<Item = RegisteredProtocol>,
	{
		let protocols: Vec<_> = protocols
			.into_iter()
			.map(|proto| (proto, SubstreamState::NotOpen))
			.collect();

		CustomProtoHandler {
			protocols,
			pending_events: VecDeque::new(),
		}
	}
}

impl<TSubstream> ProtocolsHandler for CustomProtoHandler<TSubstream>
where TSubstream: AsyncRead + AsyncWrite + 'static,
{
	type InEvent = ();		// TODO: `!`
	type OutEvent = CustomProtoHandlerEvent;
	type Substream = TSubstream;
	type Protocol = RegisteredProtocols;
	type OutboundOpenInfo = ProtocolId;

	fn listen_protocol(&self) -> Self::Protocol {
		RegisteredProtocols(self.protocols.iter().map(|s| s.0.clone()).collect())
	}

	fn inject_fully_negotiated(&mut self, newly_opened: <Self::Protocol as ConnectionUpgrade<TSubstream>>::Output, endpoint: NodeHandlerEndpoint<Self::OutboundOpenInfo>) {
		let pos = self.substreams.iter().position(|p| p.0.protocol_id() == newly_opened.protocol_id())
			// TODO: wrong proof
			.expect("The user of the ProtocolsHandler trait must always give back the value \
					 returned from poll() ; the value produced by poll() is extracted from \
					 self.substreams.0 ; self.substreams.0 is never modified ; therefore we \
					 should value back in the list ; qed");
		
		// If we already have a substream for this protocol, return.
		match self.substreams[pos].1 {
			SubstreamState::Open(existing) => {
				// Upgrade to the new version if a better version is opened.
				debug_assert_eq!(existing.protocol_id(), newly_opened.protocol_id());
				if existing.protocol_version() < newly_opened.protocol_version() {
					self.pending_events.push_back(CustomProtoHandlerEvent::ClosedCustomProtocol {
						protocol: existing.protocol_id(),
						version: existing.protocol_version(),
					});
					self.pending_events.push_back(CustomProtoHandlerEvent::OpenedCustomProtocol {
						protocol: newly_opened.protocol_id(),
						version: newly_opened.protocol_version(),
					});
					*existing = SubstreamState::Open(newly_opened);
				}
			},
			vacant @ _ => {
				self.pending_events.push_back(CustomProtoHandlerEvent::OpenedCustomProtocol {
					protocol: newly_opened.protocol_id(),
					version: newly_opened.protocol_version(),
				});
				*vacant = SubstreamState::Open(newly_opened);
			}
		}
	}

	#[inline]
	fn inject_event(&mut self, _: ()) {
	}

	#[inline]
	fn inject_inbound_closed(&mut self) {
	}

	#[inline]
	fn inject_dial_upgrade_error(&mut self, protocol_id: ProtocolId, _: &io::Error) {
		let pos = self.substreams.iter().position(|p| p.0.protocol_id() == protocol_id)
			.expect("The user of the ProtocolsHandler trait must always give back the value \
					 returned from poll() ; the value produced by poll() is extracted from \
					 self.substreams.0 ; self.substreams.0 is never modified ; therefore we \
					 should value back in the list ; qed");

		self.substreams[pos].1 = SubstreamState::Errored;
		if self.substreams.iter().all(|substream| substream.is_error()) {
			self.pending_events.push_back(CustomProtoHandlerEvent::UselessNode);
			self.shutdown = true;
		}
	}

	#[inline]
	fn shutdown(&mut self) {
		self.shutting_down = true;
	}

	fn poll(&mut self) -> Poll<Option<NodeHandlerEvent<(Self::Protocol, ProtocolId), CustomProtoHandlerEvent>>, io::Error> {
		// Process the events queue first.
		if let Some(event) = self.pending_events.pop_front() {
			return Ok(Async::Ready(Some(event)));
		}

		// Special case if shutting down.
		if self.shutting_down {
			let mut shutdown_blocked = false;

			for substream in self.substreams.iter_mut() {
				match mem::replace(&mut substream.1, SubstreamState::Closed) {
					SubstreamState::Open(stream) => match stream.close() {
						Ok(Async::NotReady) => {
							shutdown_blocked = true;
							substream.1 = SubstreamState::Open(stream);
						},
						Ok(Async::Ready(())) => {},
						Err(_) => {},
					},
					_ => (),
				}
			}

			if !shutdown_blocked {
				return Ok(Async::Ready(None));
			} else {
				return Ok(Async::NotReady);
			}
		}

		// Open substreams if necessary.
		for substream in self.substreams.iter_mut() {
			let need_open = match substream.1 {
				SubstreamState::NotOpen => true,
				_ => false
			};

			if need_open {
				substream.1 = SubstreamState::Dialed;
				let proto = RegisteredProtocols(vec![substream.0.clone()]);
				let ev = NodeHandlerEvent::OutboundSubstreamRequest(proto, substream.0.protocol_id());
				return Ok(Async::Ready(ev));
			}
		}

		// Poll for messages on the custom protocol stream.
		for substream in self.substreams.iter_mut() {
			let mut custom_proto = match mem::replace(&mut substream.1, SubstreamState::Errored) {
				SubstreamState::Open(s) => s,
				_ => continue
			};

			match custom_proto.poll() {
				Ok(Async::NotReady) => {
					substream.1 = SubstreamState::Open(custom_proto)
				},
				Ok(Async::Ready(Some(data))) => {
					let protocol_id = custom_proto.protocol_id();
					substream.1 = SubstreamState::Open(custom_proto);
					return Ok(Async::Ready(Some(CustomProtoHandlerEvent::CustomMessage {
						protocol_id,
						data,
					})));
				},
				Ok(Async::Ready(None)) => {
					return Ok(Async::Ready(Some(CustomProtoHandlerEvent::CustomProtocolClosed {
						protocol_id: custom_proto.protocol_id(),
						result: Ok(()),
					})))
				},
				Err(err) => {
					return Ok(Async::Ready(Some(CustomProtoHandlerEvent::CustomProtocolClosed {
						protocol_id: custom_proto.protocol_id(),
						result: Err(err),
					})))
				},
			}
		}

		Ok(Async::NotReady)
	}
}
