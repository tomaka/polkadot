// Copyright 2019 Parity Technologies (UK) Ltd.
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

//! Substrate offchain workers.
//!
//! The offchain workers is a special function of the runtime that
//! gets executed after block is imported. During execution
//! it's able to asynchronously submit extrinsics that will either
//! be propagated to other nodes added to the next block
//! produced by the node as unsigned transactions.
//!
//! Offchain workers can be used for computation-heavy tasks
//! that are not feasible for execution during regular block processing.
//! It can either be tasks that no consensus is required for,
//! or some form of consensus over the data can be built on-chain
//! for instance via:
//! 1. Challenge period for incorrect computations
//! 2. Majority voting for results
//! 3. etc

#![warn(missing_docs)]

use std::{
	fmt,
	marker::PhantomData,
	sync::Arc,
};

use client::runtime_api::ApiExt;
use log::{debug, warn};
use primitives::ExecutionContext;
use runtime_primitives::{
	generic::BlockId,
	traits::{self, ProvideRuntimeApi},
};
use futures::future::Future;
use transaction_pool::txpool::{Pool, ChainApi};

mod api;

pub mod testing;

pub use offchain_primitives::OffchainWorkerApi;

/// An offchain workers manager.
pub struct OffchainWorkers<C, S, Block: traits::Block> {
	client: Arc<C>,
	db: S,
	_block: PhantomData<Block>,
}

impl<C, S, Block: traits::Block> OffchainWorkers<C, S, Block> {
	/// Creates new `OffchainWorkers`.
	pub fn new(
		client: Arc<C>,
		db: S,
	) -> Self {
		Self {
			client,
			db,
			_block: PhantomData,
		}
	}
}

impl<C, S, Block: traits::Block> fmt::Debug for OffchainWorkers<C, S, Block> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_tuple("OffchainWorkers").finish()
	}
}

impl<C, S, Block> OffchainWorkers<C, S, Block> where
	Block: traits::Block,
	S: client::backend::OffchainStorage + 'static,
	C: ProvideRuntimeApi,
	C::Api: OffchainWorkerApi<Block>,
{
	/// Start the offchain workers after given block.
	#[must_use]
	pub fn on_block_imported<A>(
		&self,
		number: &<Block::Header as traits::Header>::Number,
		pool: &Arc<Pool<A>>,
	) -> impl Future<Output = ()> where
		A: ChainApi<Block=Block> + 'static,
	{
		let runtime = self.client.runtime_api();
		let at = BlockId::number(*number);
		let has_api = runtime.has_api::<dyn OffchainWorkerApi<Block>>(&at);
		debug!("Checking offchain workers at {:?}: {:?}", at, has_api);

		if has_api.unwrap_or(false) {
			let (api, runner) = api::AsyncApi::new(
				pool.clone(),
				self.db.clone(),
				at.clone(),
			);
			debug!("Running offchain workers at {:?}", at);
			let api = Box::new(api);
			runtime.offchain_worker_with_context(&at, ExecutionContext::OffchainWorker(api), *number).unwrap();
			futures::future::Either::Left(runner.process())
		} else {
			futures::future::Either::Right(futures::future::ready(()))
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use futures::Future;

	#[test]
	fn should_call_into_runtime_and_produce_extrinsic() {
		// given
		let _ = env_logger::try_init();
		let runtime = tokio::runtime::Runtime::new().unwrap();
		let client = Arc::new(test_client::new());
		let pool = Arc::new(Pool::new(Default::default(), ::transaction_pool::ChainApi::new(client.clone())));
		let db = client_db::offchain::LocalStorage::new_test();

		// when
		let offchain = OffchainWorkers::new(client, db);
		runtime.executor().spawn(offchain.on_block_imported(&0u64, &pool));

		// then
		runtime.shutdown_on_idle().wait().unwrap();
		assert_eq!(pool.status().ready, 1);
		assert_eq!(pool.ready().next().unwrap().is_propagateable(), false);
	}
}
