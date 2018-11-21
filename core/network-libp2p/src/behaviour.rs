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
use libp2p::core::PeerId;
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
}
