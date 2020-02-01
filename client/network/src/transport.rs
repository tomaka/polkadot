// Copyright 2018-2020 Parity Technologies (UK) Ltd.
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
use libp2p::{
	InboundUpgradeExt, OutboundUpgradeExt, PeerId, Transport,
	mplex, identity, secio, yamux, bandwidth, wasm_ext
};
#[cfg(not(target_os = "unknown"))]
use libp2p::{tcp, dns, websocket, noise};
#[cfg(not(target_os = "unknown"))]
use libp2p::core::{either::EitherError, either::EitherOutput};
use libp2p::core::{self, upgrade, transport::boxed::Boxed, transport::OptionalTransport, muxing::StreamMuxerBox};
use std::{io, sync::Arc, time::Duration, usize};

pub use self::bandwidth::BandwidthSinks;

/// Builds the transport that serves as a common ground for all connections.
///
/// If `memory_only` is true, then only communication within the same process are allowed. Only
/// addresses with the format `/memory/...` are allowed.
///
/// Returns a `BandwidthSinks` object that allows querying the average bandwidth produced by all
/// the connections spawned with this transport.
pub fn build_transport(
	keypair: identity::Keypair,
	memory_only: bool,
	wasm_external_transport: Option<wasm_ext::ExtTransport>
) -> (Boxed<(PeerId, StreamMuxerBox), io::Error>, Arc<bandwidth::BandwidthSinks>) {
	// Build configuration objects for encryption mechanisms.
	#[cfg(not(target_os = "unknown"))]
	let noise_config = {
		let noise_keypair = noise::Keypair::new().into_authentic(&keypair)
			// For more information about this panic, see in "On the Importance of Checking
			// Cryptographic Protocols for Faults" by Dan Boneh, Richard A. DeMillo,
			// and Richard J. Lipton.
			.expect("can only fail in case of a hardware bug; since this signing is performed only \
				once and at initialization, we're taking the bet that the inconvenience of a very \
				rare panic here is basically zero");
		noise::NoiseConfig::ix(noise_keypair)
	};
	let secio_config = secio::SecioConfig::new(keypair);

	// Build configuration objects for multiplexing mechanisms.
	let mut mplex_config = mplex::MplexConfig::new();
	mplex_config.max_buffer_len_behaviour(mplex::MaxBufferBehaviour::Block);
	mplex_config.max_buffer_len(usize::MAX);
	let yamux_config = yamux::Config::default();

	// Build the base layer of the transport.
	let sinks; // TODO: fix
	let transport = if let Some(t) = wasm_external_transport {
		// TODO: that's a hack
		let (t, s) = bandwidth::BandwidthLogging::new(t, Duration::from_secs(5));
		sinks = s;

		let t = t.and_then(move |stream, endpoint| {
			core::upgrade::apply(stream, secio_config, endpoint, upgrade::Version::V1)
				.map_ok(|(remote_id, out)| (out, remote_id))
		});

		OptionalTransport::some(t)
	} else {
		OptionalTransport::none()
	};

	#[cfg(not(target_os = "unknown"))]
	let transport = transport.or_transport(if !memory_only {
		let desktop_trans = tcp::TcpConfig::new()
			.and_then(move |stream, endpoint| {
				core::upgrade::apply(stream, noise_config, endpoint, upgrade::Version::V1)
					.and_then(|(remote_id, out)| async move {
						let remote_key = match remote_id {
							noise::RemoteIdentity::IdentityKey(key) => key,
							_ => return Err(upgrade::UpgradeError::Apply(EitherError::A(noise::NoiseError::InvalidKey)))
						};
						Ok((out, remote_key.into_peer_id()))
					})
			});

		let ws_trans = websocket::WsConfig::new(tcp::TcpConfig::new())
			.and_then(move |stream, endpoint| {
				core::upgrade::apply(stream, secio_config, endpoint, upgrade::Version::V1)
					.map_ok(|(remote_id, out)| (out, remote_id))
			});

		let final_trans = ws_trans.or_transport(desktop_trans)
			.map(|out, _| {
				match out {
					EitherOutput::First((a, b)) => (EitherOutput::First(a), b),
					EitherOutput::Second((a, b)) => (EitherOutput::Second(a), b),
				}
			});
		OptionalTransport::some(if let Ok(dns) = dns::DnsConfig::new(desktop_trans.clone()) {
			dns.boxed()
		} else {
			desktop_trans.map_err(dns::DnsErr::Underlying).boxed()
		})
	} else {
		OptionalTransport::none()
	})
	.map(|out, _| {
		match out {
			EitherOutput::First((a, b)) => (EitherOutput::First(a), b),
			EitherOutput::Second((a, b)) => (EitherOutput::Second(a), b),
		}
	});

	let transport = transport.or_transport(if memory_only {
		let mem = libp2p::core::transport::MemoryTransport::default()
			.and_then(move |stream, endpoint| {
				core::upgrade::apply(stream, noise_config, endpoint, upgrade::Version::V1)
					.and_then(|(remote_id, out)| async move {
						let remote_key = match remote_id {
							noise::RemoteIdentity::IdentityKey(key) => key,
							_ => return Err(upgrade::UpgradeError::Apply(EitherError::A(noise::NoiseError::InvalidKey)))
						};
						Ok((out, remote_key.into_peer_id()))
					})
			});

		OptionalTransport::some(mem)
	} else {
		OptionalTransport::none()
	});

	// TODO: uuuuugh
	let transport = transport
		.and_t(|out, _| {
			match out {
				EitherOutput::First((a, b)) => (EitherOutput::First(a), b),
				EitherOutput::Second((a, b)) => (EitherOutput::Second(a), b),
			}
		});

	// Multiplexing
	let transport = transport
		.and_then(move |(stream, peer_id), endpoint| {
			let peer_id2 = peer_id.clone();
			let upgrade = core::upgrade::SelectUpgrade::new(yamux_config, mplex_config)
				.map_inbound(move |muxer| (peer_id, muxer))
				.map_outbound(move |muxer| (peer_id2, muxer));

			core::upgrade::apply(stream, upgrade, endpoint, upgrade::Version::V1)
				.map_ok(|(id, muxer)| (id, core::muxing::StreamMuxerBox::new(muxer)))
		})

		.timeout(Duration::from_secs(20))
		.map_err(|err| io::Error::new(io::ErrorKind::Other, err))
		.boxed();

	(transport, sinks)
}
