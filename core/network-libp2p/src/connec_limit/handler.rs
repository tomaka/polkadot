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

use futures::prelude::*;
use libp2p::core::{
	ProtocolsHandler, ProtocolsHandlerEvent,
	upgrade::DeniedUpgrade
};
use std::{io, marker::PhantomData};
use tokio_io::{AsyncRead, AsyncWrite};
use void::{self, Void};

/// Protocol handler that tries to maintain one substream per registered custom protocol.
#[derive(Debug)]
pub struct ConnecLimitHandler<TSubstream> {
	/// `True` if we need to shut down.
	shutting_down: bool,

	/// Phantom data to pin the generic.
	marker: PhantomData<TSubstream>,
}

impl<TSubstream> ConnecLimitHandler<TSubstream> {
	/// Builds a new `ConnecLimitHandler`.
	#[inline]
	pub fn new() -> Self {
		ConnecLimitHandler {
			shutting_down: false,
			marker: PhantomData,
		}
	}
}

impl<TSubstream> ProtocolsHandler for ConnecLimitHandler<TSubstream>
where
	TSubstream: AsyncRead + AsyncWrite,
{
	type InEvent = Void;
	type OutEvent = Void;
	type Substream = TSubstream;
	type InboundProtocol = DeniedUpgrade;
	type OutboundProtocol = DeniedUpgrade;
	type OutboundOpenInfo = ();

	#[inline]
	fn listen_protocol(&self) -> Self::InboundProtocol {
		DeniedUpgrade
	}

	fn inject_fully_negotiated_inbound(
		&mut self,
		proto: Void
	) {
		void::unreachable(proto)
	}

	#[inline]
	fn inject_fully_negotiated_outbound(
		&mut self,
		proto: Void,
		_: Self::OutboundOpenInfo
	) {
		void::unreachable(proto)
	}

	fn inject_event(&mut self, message: Void) {
		void::unreachable(message)
	}

	#[inline]
	fn inject_inbound_closed(&mut self) {}

	#[inline]
	fn inject_dial_upgrade_error(&mut self, _: Self::OutboundOpenInfo, _: io::Error) {
	}

	#[inline]
	fn shutdown(&mut self) {
		self.shutting_down = true;
	}

	fn poll(
		&mut self,
	) -> Poll<
		Option<ProtocolsHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::OutEvent>>,
		io::Error,
	> {
		if self.shutting_down {
			return Ok(Async::Ready(None));
		}

		Ok(Async::NotReady)
	}
}
