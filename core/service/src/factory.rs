// Copyright 2017-2019 Parity Technologies (UK) Ltd.
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

use crate::{AuthorityKeyProvider, NetworkStatus, NetworkState, error::Error, DEFAULT_PROTOCOL_ID};
use crate::{SpawnTaskHandle, start_rpc_servers, build_network_future, components::maintain_transaction_pool};
use crate::{AbstractService, TelemetryOnConnectNotifications, RpcSession, TransactionPoolAdapter};
use crate::config::{Configuration, Roles};
use client::{BlockchainEvents, Client, runtime_api};
use consensus_common::import_queue::{ImportQueue, IncomingBlock, Link, BlockImportError, BlockImportResult};
use consensus_common::BlockOrigin;
use exit_future::Signal;
use futures::{prelude::*, future::Executor, sync::mpsc};
use futures03::{StreamExt as _, TryStreamExt as _};
use keystore::Store as Keystore;
use log::{debug, info, warn, error};
use network::{FinalityProofProvider, OnDemand, NetworkService};
use network::{config::BoxFinalityProofRequestBuilder, specialization::NetworkSpecialization};
use parity_codec::{Decode, Encode};
use parking_lot::Mutex;
use primitives::{Blake2Hasher, H256, Hasher, Pair, ed25519};
use runtime_primitives::{BuildStorage, generic::SignedBlock, generic::BlockId};
use runtime_primitives::traits::{Block as BlockT, NumberFor, Zero, One, ProvideRuntimeApi, Header, SaturatedConversion};
use substrate_executor::{NativeExecutor, NativeExecutionDispatch};
use serde::{Serialize, de::DeserializeOwned};
use std::{marker::PhantomData, io::Read, io::Write, sync::Arc};
use sysinfo::{get_current_pid, ProcessExt, System, SystemExt};
use tel::{telemetry, SUBSTRATE_INFO};
use transaction_pool::txpool::{ChainApi, Pool as TransactionPool};

/// Aggregator for the components required to build a service.
pub struct ServiceBuilder<TBl, TRtApi, TCfg, TGen, TCl, TFchr, TSc, TImpQu, TFprb, TFpp, TNetP, TExPool> {
	config: Configuration<TCfg, TGen>,
	client: Arc<TCl>,
	fetcher: Option<TFchr>,
	select_chain: Option<TSc>,
	import_queue: TImpQu,
	finality_proof_request_builder: Option<TFprb>,
	finality_proof_provider: Option<TFpp>,
	network_protocol: TNetP,
	transaction_pool: TExPool,
	marker: PhantomData<(TBl, TRtApi)>,
}

impl<TCfg, TGen> ServiceBuilder<(), (), TCfg, TGen, (), (), (), (), (), (), (), ()>
where TGen: Serialize + DeserializeOwned + BuildStorage {
	/// Start the service builder with a configuration.
	pub fn new_full<TBl: BlockT<Hash=H256>, TRtApi, TExecDisp: NativeExecutionDispatch>(
		config: Configuration<TCfg, TGen>
	) -> Result<ServiceBuilder<
		TBl,
		TRtApi,
		TCfg,
		TGen,
		Client<
			client_db::Backend<TBl>,
			client::LocalCallExecutor<client_db::Backend<TBl>, NativeExecutor<TExecDisp>>,
			TBl,
			TRtApi
		>,
		Arc<OnDemand<TBl>>,
		(),
		(),
		BoxFinalityProofRequestBuilder<TBl>,
		(),
		(),
		()
	>, Error> {
		let db_settings = client_db::DatabaseSettings {
			cache_size: None,
			state_cache_size: config.state_cache_size,
			state_cache_child_ratio:
				config.state_cache_child_ratio.map(|v| (v, 100)),
			path: config.database_path.clone(),
			pruning: config.pruning.clone(),
		};

		let executor = NativeExecutor::<TExecDisp>::new(config.default_heap_pages);

		let client = Arc::new(client_db::new_client(
			db_settings,
			executor,
			&config.chain_spec,
			config.execution_strategies.clone(),
		)?);

		Ok(ServiceBuilder {
			config,
			client,
			fetcher: None,
			select_chain: None,
			import_queue: (),
			finality_proof_request_builder: None,
			finality_proof_provider: None,
			network_protocol: (),
			transaction_pool: (),
			marker: PhantomData,
		})
	}

	/// Start the service builder with a configuration.
	pub fn new_light<TBl: BlockT<Hash=H256>, TRtApi, TExecDisp: NativeExecutionDispatch + 'static>(
		config: Configuration<TCfg, TGen>
	) -> Result<ServiceBuilder<
		TBl,
		TRtApi,
		TCfg,
		TGen,
		Client<
			client::light::backend::Backend<client_db::light::LightStorage<TBl>, network::OnDemand<TBl>, Blake2Hasher>,
			client::light::call_executor::RemoteOrLocalCallExecutor<
				TBl,
				client::light::backend::Backend<
					client_db::light::LightStorage<TBl>,
					network::OnDemand<TBl>,
					Blake2Hasher
				>,
				client::light::call_executor::RemoteCallExecutor<
					client::light::blockchain::Blockchain<
						client_db::light::LightStorage<TBl>,
						network::OnDemand<TBl>
					>,
					network::OnDemand<TBl>,
				>,
				client::LocalCallExecutor<
					client::light::backend::Backend<
						client_db::light::LightStorage<TBl>,
						network::OnDemand<TBl>,
						Blake2Hasher
					>,
					NativeExecutor<TExecDisp>
				>
			>,
			TBl,
			TRtApi
		>,
		Arc<OnDemand<TBl>>,
		(),
		(),
		BoxFinalityProofRequestBuilder<TBl>,
		(),
		(),
		()
	>, Error> {
		let db_settings = client_db::DatabaseSettings {
			cache_size: config.database_cache_size.map(|u| u as usize),
			state_cache_size: config.state_cache_size,
			state_cache_child_ratio:
				config.state_cache_child_ratio.map(|v| (v, 100)),
			path: config.database_path.clone(),
			pruning: config.pruning.clone(),
		};

		let executor = NativeExecutor::<TExecDisp>::new(config.default_heap_pages);

		let db_storage = client_db::light::LightStorage::new(db_settings)?;
		let light_blockchain = client::light::new_light_blockchain(db_storage);
		let fetch_checker = Arc::new(client::light::new_fetch_checker(light_blockchain.clone(), executor.clone()));
		let fetcher = Arc::new(network::OnDemand::new(fetch_checker));
		let client_backend = client::light::new_light_backend(light_blockchain, fetcher.clone());
		let client = client::light::new_light(client_backend, fetcher.clone(), &config.chain_spec, executor)?;

		Ok(ServiceBuilder {
			config,
			client: Arc::new(client),
			fetcher: Some(fetcher),
			select_chain: None,
			import_queue: (),
			finality_proof_request_builder: None,
			finality_proof_provider: None,
			network_protocol: (),
			transaction_pool: (),
			marker: PhantomData,
		})
	}
}

impl<TBl, TRtApi, TCfg, TGen, TCl, TFchr, TSc, TImpQu, TFprb, TFpp, TNetP, TExPool>
	ServiceBuilder<TBl, TRtApi, TCfg, TGen, TCl, TFchr, TSc, TImpQu, TFprb, TFpp, TNetP, TExPool> {

	/// Defines which head-of-chain strategy to use.
	pub fn with_opt_select_chain<USc>(
		mut self,
		select_chain_builder: impl FnOnce(&mut Configuration<TCfg, TGen>, Arc<TCl>) -> Result<Option<USc>, Error>
	) -> Result<ServiceBuilder<TBl, TRtApi, TCfg, TGen, TCl, TFchr, USc, TImpQu, TFprb, TFpp, TNetP, TExPool>, Error> {
		let select_chain = select_chain_builder(&mut self.config, self.client.clone())?;

		Ok(ServiceBuilder {
			config: self.config,
			client: self.client,
			fetcher: self.fetcher,
			select_chain,
			import_queue: self.import_queue,
			finality_proof_request_builder: self.finality_proof_request_builder,
			finality_proof_provider: self.finality_proof_provider,
			network_protocol: self.network_protocol,
			transaction_pool: self.transaction_pool,
			marker: self.marker,
		})
	}

	/// Defines which head-of-chain strategy to use.
	pub fn with_select_chain<USc>(
		self,
		builder: impl FnOnce(&mut Configuration<TCfg, TGen>, Arc<TCl>) -> Result<USc, Error>
	) -> Result<ServiceBuilder<TBl, TRtApi, TCfg, TGen, TCl, TFchr, USc, TImpQu, TFprb, TFpp, TNetP, TExPool>, Error> {
		self.with_opt_select_chain(|cfg, cl| builder(cfg, cl).map(Option::Some))
	}

	/// Defines which import queue to use.
	pub fn with_import_queue<UImpQu>(
		mut self,
		builder: impl FnOnce(&mut Configuration<TCfg, TGen>, Arc<TCl>, Option<TSc>) -> Result<UImpQu, Error>
	) -> Result<ServiceBuilder<TBl, TRtApi, TCfg, TGen, TCl, TFchr, TSc, UImpQu, TFprb, TFpp, TNetP, TExPool>, Error>
	where TSc: Clone {
		let import_queue = builder(&mut self.config, self.client.clone(), self.select_chain.clone())?;

		Ok(ServiceBuilder {
			config: self.config,
			client: self.client,
			fetcher: self.fetcher,
			select_chain: self.select_chain,
			import_queue,
			finality_proof_request_builder: self.finality_proof_request_builder,
			finality_proof_provider: self.finality_proof_provider,
			network_protocol: self.network_protocol,
			transaction_pool: self.transaction_pool,
			marker: self.marker,
		})
	}

	/// Defines which network protocol to use.
	pub fn with_network_protocol<UNetP>(
		self,
		network_protocol_builder: impl FnOnce(&Configuration<TCfg, TGen>) -> Result<UNetP, Error>
	) -> Result<ServiceBuilder<TBl, TRtApi, TCfg, TGen, TCl, TFchr, TSc, TImpQu, TFprb, TFpp, UNetP, TExPool>, Error> {
		let network_protocol = network_protocol_builder(&self.config)?;

		Ok(ServiceBuilder {
			config: self.config,
			client: self.client,
			fetcher: self.fetcher,
			select_chain: self.select_chain,
			import_queue: self.import_queue,
			finality_proof_request_builder: self.finality_proof_request_builder,
			finality_proof_provider: self.finality_proof_provider,
			network_protocol,
			transaction_pool: self.transaction_pool,
			marker: self.marker,
		})
	}

	/// Defines which strategy to use for providing finality proofs.
	pub fn with_opt_finality_proof_provider(
		self,
		builder: impl FnOnce(Arc<TCl>) -> Result<Option<Arc<FinalityProofProvider<TBl>>>, Error>
	) -> Result<ServiceBuilder<
		TBl,
		TRtApi,
		TCfg,
		TGen,
		TCl,
		TFchr,
		TSc,
		TImpQu,
		TFprb,
		Arc<FinalityProofProvider<TBl>>,
		TNetP,
		TExPool
	>, Error> {
		let finality_proof_provider = builder(self.client.clone())?;

		Ok(ServiceBuilder {
			config: self.config,
			client: self.client,
			fetcher: self.fetcher,
			select_chain: self.select_chain,
			import_queue: self.import_queue,
			finality_proof_request_builder: self.finality_proof_request_builder,
			finality_proof_provider,
			network_protocol: self.network_protocol,
			transaction_pool: self.transaction_pool,
			marker: self.marker,
		})
	}

	/// Defines which strategy to use for providing finality proofs.
	pub fn with_finality_proof_provider(
		self,
		build: impl FnOnce(Arc<TCl>) -> Result<Arc<FinalityProofProvider<TBl>>, Error>
	) -> Result<ServiceBuilder<
		TBl,
		TRtApi,
		TCfg,
		TGen,
		TCl,
		TFchr,
		TSc,
		TImpQu,
		TFprb,
		Arc<FinalityProofProvider<TBl>>,
		TNetP,
		TExPool
	>, Error> {
		self.with_opt_finality_proof_provider(|client| build(client).map(Option::Some))
	}

	/// Defines which import queue to use.
	pub fn with_import_queue_and_opt_fprb<UImpQu, UFprb>(
		mut self,
		builder: impl FnOnce(&mut Configuration<TCfg, TGen>, Arc<TCl>, Option<TSc>)
			-> Result<(UImpQu, Option<UFprb>), Error>
	) -> Result<ServiceBuilder<TBl, TRtApi, TCfg, TGen, TCl, TFchr, TSc, UImpQu, UFprb, TFpp, TNetP, TExPool>, Error>
	where TSc: Clone {
		let (import_queue, fprb) = builder(&mut self.config, self.client.clone(), self.select_chain.clone())?;

		Ok(ServiceBuilder {
			config: self.config,
			client: self.client,
			fetcher: self.fetcher,
			select_chain: self.select_chain,
			import_queue,
			finality_proof_request_builder: fprb,
			finality_proof_provider: self.finality_proof_provider,
			network_protocol: self.network_protocol,
			transaction_pool: self.transaction_pool,
			marker: self.marker,
		})
	}

	/// Defines which import queue to use.
	pub fn with_import_queue_and_fprb<UImpQu, UFprb>(
		self,
		builder: impl FnOnce(&mut Configuration<TCfg, TGen>, Arc<TCl>, Option<TSc>) -> Result<(UImpQu, UFprb), Error>
	) -> Result<ServiceBuilder<TBl, TRtApi, TCfg, TGen, TCl, TFchr, TSc, UImpQu, UFprb, TFpp, TNetP, TExPool>, Error>
	where TSc: Clone {
		self.with_import_queue_and_opt_fprb(|cfg, cl, sc| builder(cfg, cl, sc).map(|(q, f)| (q, Some(f))))
	}

	/// Defines which transaction pool to use.
	pub fn with_transaction_pool<UExPool>(
		self,
		transaction_pool_builder: impl FnOnce(transaction_pool::txpool::Options, Arc<TCl>) -> Result<UExPool, Error>
	) -> Result<ServiceBuilder<TBl, TRtApi, TCfg, TGen, TCl, TFchr, TSc, TImpQu, TFprb, TFpp, TNetP, UExPool>, Error> {
		let transaction_pool = transaction_pool_builder(self.config.transaction_pool.clone(), self.client.clone())?;

		Ok(ServiceBuilder {
			config: self.config,
			client: self.client,
			fetcher: self.fetcher,
			select_chain: self.select_chain,
			import_queue: self.import_queue,
			finality_proof_request_builder: self.finality_proof_request_builder,
			finality_proof_provider: self.finality_proof_provider,
			network_protocol: self.network_protocol,
			transaction_pool,
			marker: self.marker,
		})
	}
}

impl<TBl, TRtApi, TCfg, TGen, TBackend, TExec, TFchr, TSc, TImpQu, TFprb, TFpp, TNetP, TExPool>
ServiceBuilder<
	TBl,
	TRtApi,
	TCfg,
	TGen,
	Client<TBackend, TExec, TBl, TRtApi>,
	TFchr,
	TSc,
	TImpQu,
	TFprb,
	TFpp,
	TNetP,
	TExPool,
> where
	TBl: BlockT<Hash = <Blake2Hasher as Hasher>::Out>,
	TGen: Serialize + DeserializeOwned + BuildStorage,
	TBackend: 'static + client::backend::Backend<TBl, Blake2Hasher> + Send,
	TExec: 'static + client::CallExecutor<TBl, Blake2Hasher> + Send + Sync + Clone,
{
	/// Revert the chain.
	pub fn revert_chain(&self, blocks: NumberFor<TBl>) -> Result<(), Error> {
		let reverted = self.client.revert(blocks)?;
		let info = self.client.info().chain;

		if reverted.is_zero() {
			info!("There aren't any non-finalized blocks to revert.");
		} else {
			info!("Reverted {} blocks. Best: #{} ({})", reverted, info.best_number, info.best_hash);
		}
		Ok(())
	}

	/// Export a range of blocks to a binary stream.
	pub fn export_blocks<E, W>(
		&self,
		exit: E,
		mut output: W,
		from: NumberFor<TBl>,
		to: Option<NumberFor<TBl>>,
		json: bool
	) -> Result<(), Error>
		where
		E: Future<Item=(),Error=()> + Send + 'static,
		W: Write,
	{
		let mut block = from;

		let last = match to {
			Some(v) if v.is_zero() => One::one(),
			Some(v) => v,
			None => self.client.info().chain.best_number,
		};

		if last < block {
			return Err("Invalid block range specified".into());
		}

		let (exit_send, exit_recv) = std::sync::mpsc::channel();
		::std::thread::spawn(move || {
			let _ = exit.wait();
			let _ = exit_send.send(());
		});
		info!("Exporting blocks from #{} to #{}", block, last);
		if !json {
			let last_: u64 = last.saturated_into::<u64>();
			let block_: u64 = block.saturated_into::<u64>();
			let len: u64 = last_ - block_ + 1;
			output.write(&len.encode())?;
		}

		loop {
			if exit_recv.try_recv().is_ok() {
				break;
			}
			match self.client.block(&BlockId::number(block))? {
				Some(block) => {
					if json {
						serde_json::to_writer(&mut output, &block)
							.map_err(|e| format!("Error writing JSON: {}", e))?;
					} else {
						output.write(&block.encode())?;
					}
				},
				None => break,
			}
			if (block % 10000.into()).is_zero() {
				info!("#{}", block);
			}
			if block == last {
				break;
			}
			block += One::one();
		}
		Ok(())
	}
}

impl<TBl, TRtApi, TCfg, TGen, TBackend, TExec, TFchr, TSc, TImpQu, TFprb, TFpp, TNetP, TExPool>
ServiceBuilder<
	TBl,
	TRtApi,
	TCfg,
	TGen,
	Client<TBackend, TExec, TBl, TRtApi>,
	TFchr,
	TSc,
	TImpQu,
	TFprb,
	TFpp,
	TNetP,
	TExPool,
> where
	TBl: BlockT<Hash = <Blake2Hasher as Hasher>::Out>,
	TGen: Serialize + DeserializeOwned + BuildStorage,
	TBackend: 'static + client::backend::Backend<TBl, Blake2Hasher> + Send,
	TExec: 'static + client::CallExecutor<TBl, Blake2Hasher> + Send + Sync + Clone,
	TImpQu: 'static + ImportQueue<TBl>,
{
	/// Returns a future that import blocks from a binary stream.
	pub fn import_blocks<'a, E, R>(
		&'a mut self,
		exit: E,
		mut input: R
	) -> Result<impl Future<Item = (), Error = ()> + 'a, Error>
		where E: Future<Item=(),Error=()> + Send + 'static, R: Read,
	{
		struct WaitLink {
			imported_blocks: u64,
		}

		impl WaitLink {
			fn new() -> WaitLink {
				WaitLink {
					imported_blocks: 0,
				}
			}
		}

		impl<B: BlockT> Link<B> for WaitLink {
			fn blocks_processed(
				&mut self,
				imported: usize,
				count: usize,
				results: Vec<(Result<BlockImportResult<NumberFor<B>>, BlockImportError>, B::Hash)>
			) {
				self.imported_blocks += imported as u64;
				if results.iter().any(|(r, _)| r.is_err()) {
					warn!("There was an error importing {} blocks", count);
				}
			}
		}

		let (exit_send, exit_recv) = std::sync::mpsc::channel();
		::std::thread::spawn(move || {
			let _ = exit.wait();
			let _ = exit_send.send(());
		});

		let count: u64 = Decode::decode(&mut input).ok_or("Error reading file")?;
		info!("Importing {} blocks", count);
		let mut block_count = 0;
		for b in 0 .. count {
			if exit_recv.try_recv().is_ok() {
				break;
			}
			if let Some(signed) = SignedBlock::<TBl>::decode(&mut input) {
				let (header, extrinsics) = signed.block.deconstruct();
				let hash = header.hash();
				// import queue handles verification and importing it into the client
				self.import_queue.import_blocks(BlockOrigin::File, vec![
					IncomingBlock::<TBl>{
						hash,
						header: Some(header),
						body: Some(extrinsics),
						justification: signed.justification,
						origin: None,
					}
				]);
			} else {
				warn!("Error reading block data at {}.", b);
				break;
			}

			block_count = b;
			if b % 1000 == 0 && b != 0 {
				info!("#{} blocks were added to the queue", b);
			}
		}

		let mut link = WaitLink::new();
		Ok(futures::future::poll_fn(move || {
			if exit_recv.try_recv().is_ok() {
				return Ok(Async::Ready(()));
			}

			let blocks_before = link.imported_blocks;
			self.import_queue.poll_actions(&mut link);
			if link.imported_blocks / 1000 != blocks_before / 1000 {
				info!(
					"#{} blocks were imported (#{} left)",
					link.imported_blocks,
					count - link.imported_blocks
				);
			}
			if link.imported_blocks >= count {
				info!("Imported {} blocks. Best: #{}", block_count, self.client.info().chain.best_number);
				Ok(Async::Ready(()))
			} else {
				Ok(Async::NotReady)
			}
		}))
	}
}

impl<TBl, TRtApi, TCfg, TGen, TBackend, TExec, TSc, TImpQu, TNetP, TExPoolApi>
ServiceBuilder<
	TBl,
	TRtApi,
	TCfg,
	TGen,
	Client<TBackend, TExec, TBl, TRtApi>,
	Arc<OnDemand<TBl>>,
	TSc,
	TImpQu,
	BoxFinalityProofRequestBuilder<TBl>,
	Arc<FinalityProofProvider<TBl>>,
	TNetP,
	TransactionPool<TExPoolApi>
> where
	Client<TBackend, TExec, TBl, TRtApi>: ProvideRuntimeApi,
	<Client<TBackend, TExec, TBl, TRtApi> as ProvideRuntimeApi>::Api:
		runtime_api::Metadata<TBl> + offchain::OffchainWorkerApi<TBl> + runtime_api::TaggedTransactionQueue<TBl>,
	TBl: BlockT<Hash = <Blake2Hasher as Hasher>::Out>,
	TRtApi: 'static + Send + Sync,
	TCfg: Default,
	TGen: Serialize + DeserializeOwned + BuildStorage,
	TBackend: 'static + client::backend::Backend<TBl, Blake2Hasher> + Send,
	TExec: 'static + client::CallExecutor<TBl, Blake2Hasher> + Send + Sync + Clone,
	TImpQu: 'static + ImportQueue<TBl>,
	TNetP: NetworkSpecialization<TBl>,
	TExPoolApi: 'static + ChainApi<Block = TBl, Hash = <TBl as BlockT>::Hash>,
{
	/// Builds the service.
	pub fn build(mut self) -> Result<Service<
		Configuration<TCfg, TGen>,
		TBl,
		Client<TBackend, TExec, TBl, TRtApi>,
		TSc,
		NetworkStatus<TBl>,
		NetworkService<TBl, TNetP, <TBl as BlockT>::Hash>,
		TransactionPool<TExPoolApi>,
		offchain::OffchainWorkers<
			Client<TBackend, TExec, TBl, TRtApi>,
			TBackend::OffchainStorage,
			AuthorityKeyProvider,
			TBl
		>,
	>, Error> {
		let (signal, exit) = ::exit_future::signal();

		// List of asynchronous tasks to spawn. We collect them, then spawn them all at once.
		let (to_spawn_tx, to_spawn_rx) =
			mpsc::unbounded::<Box<dyn Future<Item = (), Error = ()> + Send>>();

		let mut keystore = if let Some(keystore_path) = self.config.keystore_path.as_ref() {
			match Keystore::open(keystore_path.clone()) {
				Ok(ks) => Some(ks),
				Err(err) => {
					error!("Failed to initialize keystore: {}", err);
					None
				}
			}
		} else {
			None
		};

		// Keep the public key for telemetry
		let public_key: String;

		// This is meant to be for testing only
		// FIXME #1063 remove this
		if let Some(keystore) = keystore.as_mut() {
			for seed in &self.config.keys {
				keystore.generate_from_seed::<ed25519::Pair>(seed)?;
			}

			public_key = match keystore.contents::<ed25519::Public>()?.get(0) {
				Some(public_key) => public_key.to_string(),
				None => {
					let key: ed25519::Pair = keystore.generate(&self.config.password.as_ref())?;
					let public_key = key.public();
					info!("Generated a new keypair: {:?}", public_key);
					public_key.to_string()
				}
			}
		} else {
			public_key = format!("<disabled-keystore>");
		}

		let import_queue = Box::new(self.import_queue);
		let chain_info = self.client.info().chain;

		let version = self.config.full_version();
		info!("Highest known block at #{}", chain_info.best_number);
		telemetry!(SUBSTRATE_INFO; "node.start";
			"height" => chain_info.best_number.saturated_into::<u64>(),
			"best" => ?chain_info.best_hash
		);

		let transaction_pool = Arc::new(self.transaction_pool);
		let transaction_pool_adapter = Arc::new(TransactionPoolAdapter {
			imports_external_transactions: !self.config.roles.is_light(),
			pool: transaction_pool.clone(),
			client: self.client.clone(),
		});

		let protocol_id = {
			let protocol_id_full = match self.config.chain_spec.protocol_id() {
				Some(pid) => pid,
				None => {
					warn!("Using default protocol ID {:?} because none is configured in the \
						chain specs", DEFAULT_PROTOCOL_ID
					);
					DEFAULT_PROTOCOL_ID
				}
			}.as_bytes();
			network::config::ProtocolId::from(protocol_id_full)
		};

		let network_params = network::config::Params {
			roles: self.config.roles,
			network_config: self.config.network.clone(),
			chain: self.client.clone(),
			finality_proof_provider: self.finality_proof_provider,
			finality_proof_request_builder: self.finality_proof_request_builder,
			on_demand: self.fetcher,
			transaction_pool: transaction_pool_adapter.clone() as _,
			import_queue,
			protocol_id,
			specialization: self.network_protocol,
		};

		let has_bootnodes = !network_params.network_config.boot_nodes.is_empty();
		let network_mut = network::NetworkWorker::new(network_params)?;
		let network = network_mut.service().clone();
		let network_status_sinks = Arc::new(Mutex::new(Vec::new()));

		let keystore_authority_key = AuthorityKeyProvider {
			roles: self.config.roles,
			password: self.config.password.clone(),
			keystore: keystore.map(Arc::new),
		};

		#[allow(deprecated)]
		let offchain_storage = self.client.backend().offchain_storage();
		let offchain_workers = match (self.config.offchain_worker, offchain_storage) {
			(true, Some(db)) => {
				Some(Arc::new(offchain::OffchainWorkers::new(
					self.client.clone(),
					db,
					keystore_authority_key.clone(),
					self.config.password.clone(),
				)))
			},
			(true, None) => {
				log::warn!("Offchain workers disabled, due to lack of offchain storage support in backend.");
				None
			},
			_ => None,
		};

		{
			// block notifications
			let txpool = Arc::downgrade(&transaction_pool);
			let wclient = Arc::downgrade(&self.client);
			let offchain = offchain_workers.as_ref().map(Arc::downgrade);
			let to_spawn_tx_ = to_spawn_tx.clone();
			let network_state = network.clone();

			let events = self.client.import_notification_stream()
				.map(|v| Ok::<_, ()>(v)).compat()
				.for_each(move |notification| {
					let number = *notification.header.number();

					if let (Some(txpool), Some(client)) = (txpool.upgrade(), wclient.upgrade()) {
						maintain_transaction_pool(
							&BlockId::hash(notification.hash),
							&*client,
							&*txpool,
						).map_err(|e| warn!("Pool error processing new block: {:?}", e))?;
					}

					if let (Some(txpool), Some(oc)) = (txpool.upgrade(), offchain.as_ref().and_then(|o| o.upgrade())) {
						let future = Box::new(oc.on_block_imported(&number, &txpool, network_state.clone())) as Box<_>;
						let _ = to_spawn_tx_.unbounded_send(future);
					}

					Ok(())
				})
				.select(exit.clone())
				.then(|_| Ok(()));
			let _ = to_spawn_tx.unbounded_send(Box::new(events));
		}

		{
			// extrinsic notifications
			let network = Arc::downgrade(&network);
			let transaction_pool_ = transaction_pool.clone();
			let events = transaction_pool.import_notification_stream()
				.for_each(move |_| {
					if let Some(network) = network.upgrade() {
						network.trigger_repropagate();
					}
					let status = transaction_pool_.status();
					telemetry!(SUBSTRATE_INFO; "txpool.import";
						"ready" => status.ready,
						"future" => status.future
					);
					Ok(())
				})
				.select(exit.clone())
				.then(|_| Ok(()));

			let _ = to_spawn_tx.unbounded_send(Box::new(events));
		}

		// Periodically notify the telemetry.
		let transaction_pool_ = transaction_pool.clone();
		let client_ = self.client.clone();
		let mut sys = System::new();
		let self_pid = get_current_pid().ok();
		let (netstat_tx, netstat_rx) = mpsc::unbounded::<(NetworkStatus<TBl>, NetworkState)>();
		network_status_sinks.lock().push(netstat_tx);
		let tel_task = netstat_rx.for_each(move |(net_status, network_state)| {
			let info = client_.info();
			let best_number = info.chain.best_number.saturated_into::<u64>();
			let best_hash = info.chain.best_hash;
			let num_peers = net_status.num_connected_peers;
			let txpool_status = transaction_pool_.status();
			let finalized_number: u64 = info.chain.finalized_number.saturated_into::<u64>();
			let bandwidth_download = net_status.average_download_per_sec;
			let bandwidth_upload = net_status.average_upload_per_sec;

			#[allow(deprecated)]
			let backend = (*client_).backend();
			let used_state_cache_size = match backend.used_state_cache_size(){
				Some(size) => size,
				None => 0,
			};

			// get cpu usage and memory usage of this process
			let (cpu_usage, memory) = if let Some(self_pid) = self_pid {
				if sys.refresh_process(self_pid) {
					let proc = sys.get_process(self_pid)
						.expect("Above refresh_process succeeds, this should be Some(), qed");
					(proc.cpu_usage(), proc.memory())
				} else { (0.0, 0) }
			} else { (0.0, 0) };

			telemetry!(
				SUBSTRATE_INFO;
				"system.interval";
				"network_state" => network_state,
				"peers" => num_peers,
				"height" => best_number,
				"best" => ?best_hash,
				"txcount" => txpool_status.ready,
				"cpu" => cpu_usage,
				"memory" => memory,
				"finalized_height" => finalized_number,
				"finalized_hash" => ?info.chain.finalized_hash,
				"bandwidth_download" => bandwidth_download,
				"bandwidth_upload" => bandwidth_upload,
				"used_state_cache_size" => used_state_cache_size,
			);

			Ok(())
		}).select(exit.clone()).then(|_| Ok(()));
		let _ = to_spawn_tx.unbounded_send(Box::new(tel_task));

		// RPC
		let (system_rpc_tx, system_rpc_rx) = mpsc::unbounded();
		let config = &self.config;
		let client = self.client.clone();
		let transaction_pool_ = transaction_pool.clone();
		let to_spawn_tx_ = to_spawn_tx.clone();
		let gen_handler = move || {
			let system_info = rpc::apis::system::SystemInfo {
				chain_name: config.chain_spec.name().into(),
				impl_name: config.impl_name.into(),
				impl_version: config.impl_version.into(),
				properties: config.chain_spec.properties(),
			};
			let task_executor = Arc::new(SpawnTaskHandle { sender: to_spawn_tx_.clone() });
			let subscriptions = rpc::apis::Subscriptions::new(task_executor.clone());
			let chain = rpc::apis::chain::Chain::new(client.clone(), subscriptions.clone());
			let state = rpc::apis::state::State::new(client.clone(), subscriptions.clone());
			let author = rpc::apis::author::Author::new(client.clone(), transaction_pool_.clone(), subscriptions);
			let system = rpc::apis::system::System::<TBl>::new(system_info.clone(), system_rpc_tx.clone());
			rpc::rpc_handler::<TBl, <TBl as BlockT>::Hash, _, _, _, _>(
				state,
				chain,
				author,
				system,
			)
		};
		let rpc_handlers = gen_handler();
		let rpc = start_rpc_servers(&self.config, gen_handler)?;

		let _ = to_spawn_tx.unbounded_send(Box::new(build_network_future(
			network_mut,
			self.client.clone(),
			network_status_sinks.clone(),
			system_rpc_rx,
			has_bootnodes
		)
			.map_err(|_| ())
			.select(exit.clone())
			.then(|_| Ok(()))));

		let telemetry_connection_sinks: Arc<Mutex<Vec<mpsc::UnboundedSender<()>>>> = Default::default();

		// Telemetry
		let telemetry_endpoints = self.config.telemetry_endpoints.clone();
		let config = &mut self.config;
		let telemetry = telemetry_endpoints.map(|endpoints| {
			let is_authority = config.roles == Roles::AUTHORITY;
			let network_id = network.local_peer_id().to_base58();
			let name = config.name.clone();
			let impl_name = config.impl_name.to_owned();
			let version = version.clone();
			let chain_name = config.chain_spec.name().to_owned();
			let telemetry_connection_sinks_ = telemetry_connection_sinks.clone();
			let telemetry = tel::init_telemetry(tel::TelemetryConfig {
				endpoints,
				wasm_external_transport: config.telemetry_external_transport.take(),
			});
			let future = telemetry.clone()
				.map(|ev| Ok::<_, ()>(ev))
				.compat()
				.for_each(move |event| {
					// Safe-guard in case we add more events in the future.
					let tel::TelemetryEvent::Connected = event;

					telemetry!(SUBSTRATE_INFO; "system.connected";
						"name" => name.clone(),
						"implementation" => impl_name.clone(),
						"version" => version.clone(),
						"config" => "",
						"chain" => chain_name.clone(),
						"pubkey" => &public_key,
						"authority" => is_authority,
						"network_id" => network_id.clone()
					);

					telemetry_connection_sinks_.lock().retain(|sink| {
						sink.unbounded_send(()).is_ok()
					});
					Ok(())
				});
			let _ = to_spawn_tx.unbounded_send(Box::new(future
				.select(exit.clone())
				.then(|_| Ok(()))));
			telemetry
		});

		Ok(Service {
			client: self.client,
			network,
			network_status_sinks,
			select_chain: self.select_chain,
			transaction_pool: transaction_pool,
			signal: Some(signal),
			to_spawn_tx,
			to_spawn_rx,
			to_poll: Vec::new(),
			keystore: keystore_authority_key,
			config: self.config,
			exit,
			rpc_handlers,
			_rpc: rpc,
			_telemetry: telemetry,
			_offchain_workers: offchain_workers,
			_telemetry_on_connect_sinks: telemetry_connection_sinks.clone(),
			marker: PhantomData,
		})
	}
}

/// Substrate service.
// Note: all the fields are `pub(crate)` at the moment because the old Service builder API needs
// to be able to instantiate a `factory::Service`. The fields should be make private in the future.
pub struct Service<TCfg, TBl, TCl, TSc, TNetStatus, TNet, TTxPool, TOc> {
	pub(crate) client: Arc<TCl>,
	pub(crate) select_chain: Option<TSc>,
	pub(crate) network: Arc<TNet>,
	/// Sinks to propagate network status updates.
	pub(crate) network_status_sinks: Arc<Mutex<Vec<mpsc::UnboundedSender<(
		TNetStatus, NetworkState
	)>>>>,
	pub(crate) transaction_pool: Arc<TTxPool>,
	pub(crate) keystore: AuthorityKeyProvider,
	pub(crate) exit: ::exit_future::Exit,
	pub(crate) signal: Option<Signal>,
	/// Sender for futures that must be spawned as background tasks.
	pub(crate) to_spawn_tx: mpsc::UnboundedSender<Box<dyn Future<Item = (), Error = ()> + Send>>,
	/// Receiver for futures that must be spawned as background tasks.
	pub(crate) to_spawn_rx: mpsc::UnboundedReceiver<Box<dyn Future<Item = (), Error = ()> + Send>>,
	/// List of futures to poll from `poll`.
	/// If spawning a background task is not possible, we instead push the task into this `Vec`.
	/// The elements must then be polled manually.
	pub(crate) to_poll: Vec<Box<dyn Future<Item = (), Error = ()> + Send>>,
	/// Configuration of this Service
	pub(crate) config: TCfg,
	pub(crate) rpc_handlers: rpc::RpcHandler,
	pub(crate) _rpc: Box<dyn std::any::Any + Send + Sync>,
	pub(crate) _telemetry: Option<tel::Telemetry>,
	pub(crate) _telemetry_on_connect_sinks: Arc<Mutex<Vec<mpsc::UnboundedSender<()>>>>,
	pub(crate) _offchain_workers: Option<Arc<TOc>>,
	pub(crate) marker: PhantomData<TBl>,
}

impl<TCfg, TBl, TBackend, TExec, TRtApi, TSc, TNet, TExPoolApi, TOc> AbstractService for
	Service<TCfg, TBl, Client<TBackend, TExec, TBl, TRtApi>, TSc, NetworkStatus<TBl>, TNet, TransactionPool<TExPoolApi>, TOc>
where TCfg: 'static + Send,
	TBl: BlockT<Hash = H256>,
	TBackend: 'static + client::backend::Backend<TBl, Blake2Hasher>,
	TExec: 'static + client::CallExecutor<TBl, Blake2Hasher> + Send + Sync + Clone,
	TRtApi: 'static + Send + Sync,
	TSc: 'static + Clone + Send,
	TNet: 'static + Send + Sync,
	TExPoolApi: 'static + ChainApi,
	TOc: 'static + Send + Sync,
{
	type Block = TBl;
	type Backend = TBackend;
	type Executor = TExec;
	type RuntimeApi = TRtApi;
	type Config = TCfg;
	type SelectChain = TSc;
	type TransactionPoolApi = TExPoolApi;
	type NetworkService = TNet;

	fn telemetry_on_connect_stream(&self) -> TelemetryOnConnectNotifications {
		self.telemetry_on_connect_stream()
	}

	fn config(&self) -> &Self::Config {
		self.config()
	}

	fn config_mut(&mut self) -> &mut Self::Config {
		self.config_mut()
	}

	fn authority_key<TPair: Pair>(&self) -> Option<TPair> {
		self.authority_key()
	}

	fn telemetry(&self) -> Option<tel::Telemetry> {
		self.telemetry()
	}

	fn spawn_task(&self, task: impl Future<Item = (), Error = ()> + Send + 'static) {
		self.spawn_task(task)
	}

	fn spawn_task_handle(&self) -> SpawnTaskHandle {
		self.spawn_task_handle()
	}

	fn rpc_query(&self, mem: &RpcSession, request: &str) -> Box<dyn Future<Item = Option<String>, Error = ()> + Send> {
		Box::new(self.rpc_query(mem, request))
	}

	fn client(&self) -> Arc<Client<Self::Backend, Self::Executor, Self::Block, Self::RuntimeApi>> {
		self.client()
	}

	fn select_chain(&self) -> Option<Self::SelectChain> {
		self.select_chain()
	}

	fn network(&self) -> Arc<Self::NetworkService> {
		self.network()
	}

	fn network_status(&self) -> mpsc::UnboundedReceiver<(NetworkStatus<Self::Block>, NetworkState)> {
		self.network_status()
	}

	fn transaction_pool(&self) -> Arc<TransactionPool<Self::TransactionPoolApi>> {
		self.transaction_pool()
	}

	fn on_exit(&self) -> ::exit_future::Exit {
		self.on_exit()
	}
}

impl<TCfg, TBl, TCl, TSc, TNetStatus, TNet, TTxPool, TOc>
Service<TCfg, TBl, TCl, TSc, TNetStatus, TNet, TTxPool, TOc>
{
	/// Get event stream for telemetry connection established events.
	pub fn telemetry_on_connect_stream(&self) -> TelemetryOnConnectNotifications {
		let (sink, stream) = mpsc::unbounded();
		self._telemetry_on_connect_sinks.lock().push(sink);
		stream
	}

	/// Returns the configuration that was passed on construction.
	pub fn config(&self) -> &TCfg {
		&self.config
	}

	/// Returns the configuration that was passed on construction.
	pub fn config_mut(&mut self) -> &mut TCfg {
		&mut self.config
	}

	/// give the authority key, if we are an authority and have a key
	pub fn authority_key<TPair: Pair>(&self) -> Option<TPair> {
		use offchain::AuthorityKeyProvider;

		self.keystore.authority_key()
	}

	/// return a shared instance of Telemetry (if enabled)
	pub fn telemetry(&self) -> Option<tel::Telemetry> {
		self._telemetry.as_ref().map(|t| t.clone())
	}

	/// Spawns a task in the background that runs the future passed as parameter.
	pub fn spawn_task(&self, task: impl Future<Item = (), Error = ()> + Send + 'static) {
		let _ = self.to_spawn_tx.unbounded_send(Box::new(task));
	}

	/// Returns a handle for spawning tasks.
	pub fn spawn_task_handle(&self) -> SpawnTaskHandle {
		SpawnTaskHandle {
			sender: self.to_spawn_tx.clone(),
		}
	}

	/// Starts an RPC query.
	///
	/// The query is passed as a string and must be a JSON text similar to what an HTTP client
	/// would for example send.
	///
	/// Returns a `Future` that contains the optional response.
	///
	/// If the request subscribes you to events, the `Sender` in the `RpcSession` object is used to
	/// send back spontaneous events.
	pub fn rpc_query(&self, mem: &RpcSession, request: &str)
	-> impl Future<Item = Option<String>, Error = ()> {
		self.rpc_handlers.handle_request(request, mem.metadata.clone())
	}

	/// Get shared client instance.
	pub fn client(&self) -> Arc<TCl> {
		self.client.clone()
	}

	/// Get clone of select chain.
	pub fn select_chain(&self) -> Option<TSc> where TSc: Clone {
		self.select_chain.clone()
	}

	/// Get shared network instance.
	pub fn network(&self) -> Arc<TNet> {
		self.network.clone()
	}

	/// Returns a receiver that periodically receives a status of the network.
	pub fn network_status(&self) -> mpsc::UnboundedReceiver<(TNetStatus, NetworkState)> {
		let (sink, stream) = mpsc::unbounded();
		self.network_status_sinks.lock().push(sink);
		stream
	}

	/// Get shared transaction pool instance.
	pub fn transaction_pool(&self) -> Arc<TTxPool> {
		self.transaction_pool.clone()
	}

	/// Get a handle to a future that will resolve on exit.
	pub fn on_exit(&self) -> ::exit_future::Exit {
		self.exit.clone()
	}
}

impl<TCfg, TBl, TCl, TSc, TNetStatus, TNet, TTxPool, TOc> Future for
Service<TCfg, TBl, TCl, TSc, TNetStatus, TNet, TTxPool, TOc> {
	type Item = ();
	type Error = ();

	fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
		while let Ok(Async::Ready(Some(task_to_spawn))) = self.to_spawn_rx.poll() {
			let executor = tokio_executor::DefaultExecutor::current();
			if let Err(err) = executor.execute(task_to_spawn) {
				debug!(
					target: "service",
					"Failed to spawn background task: {:?}; falling back to manual polling",
					err
				);
				self.to_poll.push(err.into_future());
			}
		}

		// Polling all the `to_poll` futures.
		while let Some(pos) = self.to_poll.iter_mut().position(|t| t.poll().map(|t| t.is_ready()).unwrap_or(true)) {
			self.to_poll.remove(pos);
		}

		// The service future never ends.
		Ok(Async::NotReady)
	}
}

impl<TCfg, TBl, TCl, TSc, TNetStatus, TNet, TTxPool, TOc> Executor<Box<dyn Future<Item = (), Error = ()> + Send>> for
Service<TCfg, TBl, TCl, TSc, TNetStatus, TNet, TTxPool, TOc> {
	fn execute(
		&self,
		future: Box<dyn Future<Item = (), Error = ()> + Send>
	) -> Result<(), futures::future::ExecuteError<Box<dyn Future<Item = (), Error = ()> + Send>>> {
		if let Err(err) = self.to_spawn_tx.unbounded_send(future) {
			let kind = futures::future::ExecuteErrorKind::Shutdown;
			Err(futures::future::ExecuteError::new(kind, err.into_inner()))
		} else {
			Ok(())
		}
	}
}

impl<TCfg, TBl, TCl, TSc, TNetStatus, TNet, TTxPool, TOc> Drop for
Service<TCfg, TBl, TCl, TSc, TNetStatus, TNet, TTxPool, TOc> {
	fn drop(&mut self) {
		debug!(target: "service", "Substrate service shutdown");
		if let Some(signal) = self.signal.take() {
			signal.fire();
		}
	}
}
