// Copyright 2017-2020 Parity Technologies (UK) Ltd.
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

use futures::{prelude::*, channel::{mpsc, oneshot}, lock::Mutex as AsyncMutex};
use sp_runtime::{Justification, generic::BlockId, traits::Block};
use std::{fmt, pin::Pin, sync::Arc, time::Duration};

pub use traits::{Client, FinalityProofProvider};

mod traits;

/// Time after which we consider an information request to have timed out.
const TIMEOUT: Duration = Duration::from_secs(60);
/// Number of workers that we are using to perform the calls to the client.
const NUM_WORKERS: u32 = 2;

/// Manages accesses to the client in a fair way.
///
/// The networking is a critical entry point to the software. Anyone can send requests asking for
/// information from our nodes. Some of these requests can accidentally or maliciously use a lot
/// of CPU or I/O.
///
/// The [`ChainInfoProvider`] wraps around the client and gives a fair access time to it,
/// distributing access time between all the peers performing incoming requests.
///
/// # Implementation
///
/// This struct maintains a background task that is responsible for calling the client. Each
/// request is sent to the background task that actually executes it and sends back the result.
///
pub struct ChainInfoProvider<B: Block> {
	to_background_task: mpsc::Sender<ToBackground<B>>,
}

/// Message delivered to the background task.
///
/// Each message corresponds to a query to the client and contains a `Sender` where the background
/// task must deliver the response.
enum ToBackground<B: Block> {
	BlockBody(BlockId<B>, oneshot::Sender<sp_blockchain::Result<Option<Vec<<B as Block>::Extrinsic>>>>),
	Justification(BlockId<B>, oneshot::Sender<sp_blockchain::Result<Option<Justification>>>),
	Header(BlockId<B>, oneshot::Sender<sp_blockchain::Result<Option<<B as Block>::Header>>>),
}

impl<B: Block> ChainInfoProvider<B> {
	/// Wraps around `client`.
	///
	/// Uses `executor` to spawn tasks to run in the background.
	pub fn new(
		client: Arc<dyn Client<B>>,
		executor: impl Fn(Pin<Box<dyn Future<Output = ()> + Send>>)
	) -> Self {
		let (to_background_task, from_frontend) = mpsc::channel(128);
		let from_frontend = Arc::new(AsyncMutex::new(from_frontend.fuse()));

		for _ in 0..NUM_WORKERS {
			executor(background_task(client.clone(), from_frontend.clone()).boxed());
		}

		ChainInfoProvider {
			to_background_task,
		}
	}

	/// Returns the header of the given block. Includes a timeout and is therefore guaranteed to
	/// yield a value in reasonable time.
	pub async fn header(&self, id: BlockId<B>) -> sp_blockchain::Result<Option<<B as Block>::Header>> {
		let timeout = futures_timer::Delay::new(TIMEOUT);

		let response = async move {
			let (tx, rx) = oneshot::channel();
			if self.to_background_task.clone().send(ToBackground::Header(id, tx)).await.is_ok() {
				match rx.await {
					Ok(v) => v,
					Err(_) => Err(build_crashed_err())
				}
			} else {
				Err(build_crashed_err())
			}
		};

		futures::pin_mut!(timeout, response);
		match future::select(timeout, response).await {
			future::Either::Left(_) => Err(build_timeout_err()),
			future::Either::Right((r, _)) => r,
		}
	}

	/// Returns the justification of the given block. Includes a timeout and is therefore
	/// guaranteed to yield a value in reasonable time.
	pub async fn justification(&self, id: BlockId<B>) -> sp_blockchain::Result<Option<Justification>> {
		let timeout = futures_timer::Delay::new(TIMEOUT);

		let response = async move {
			let (tx, rx) = oneshot::channel();
			if self.to_background_task.clone().send(ToBackground::Justification(id, tx)).await.is_ok() {
				match rx.await {
					Ok(v) => v,
					Err(_) => Err(build_crashed_err())
				}
			} else {
				Err(build_crashed_err())
			}
		};

		futures::pin_mut!(timeout, response);
		match future::select(timeout, response).await {
			future::Either::Left(_) => Err(build_timeout_err()),
			future::Either::Right((r, _)) => r,
		}
	}

	/// Returns the body of the given block. Includes a timeout and is therefore guaranteed to
	/// yield a value in reasonable time.
	pub async fn block_body(
		&self,
		id: BlockId<B>
	) -> sp_blockchain::Result<Option<Vec<<B as Block>::Extrinsic>>> {
		let timeout = futures_timer::Delay::new(TIMEOUT);

		let response = async move {
			let (tx, rx) = oneshot::channel();
			if self.to_background_task.clone().send(ToBackground::BlockBody(id, tx)).await.is_ok() {
				match rx.await {
					Ok(v) => v,
					Err(_) => Err(build_crashed_err())
				}
			} else {
				Err(build_crashed_err())
			}
		};

		futures::pin_mut!(timeout, response);
		match future::select(timeout, response).await {
			future::Either::Left(_) => Err(build_timeout_err()),
			future::Either::Right((r, _)) => r,
		}
	}
}

impl<B: Block> fmt::Debug for ChainInfoProvider<B> {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.debug_tuple("ChainInfoProvider").finish()
	}
}

/// Builds an error indicating that the background task has crashed.
fn build_crashed_err() -> sp_blockchain::Error {
	sp_blockchain::Error::Msg(From::from("ChainInfoProvider background task has crashed"))
}

/// Builds an error indicating that a request has timed out.
fn build_timeout_err() -> sp_blockchain::Error {
	sp_blockchain::Error::Msg(From::from("Timeout in ChainInfoProvider"))
}

/// Background task that is run in parallel of the [`ChainInfoProvider`].
async fn background_task<B: Block>(
	chain: Arc<dyn Client<B>>,
	tasks_receiver: Arc<AsyncMutex<stream::Fuse<mpsc::Receiver<ToBackground<B>>>>>
) {
	loop {
		let next = {
			let mut lock = tasks_receiver.lock().await;
			lock.next().await
		};

		match next {
			// Frontend has shut down. End the background task.
			None => break,

			Some(ToBackground::BlockBody(id, sender)) => {
				let response = chain.block_body(&id);
				let _ = sender.send(response);
			},
			Some(ToBackground::Justification(id, sender)) => {
				let response = chain.justification(&id);
				let _ = sender.send(response);
			},
			Some(ToBackground::Header(id, sender)) => {
				let response = chain.header(id);
				let _ = sender.send(response);
			},
		}
	}
}
