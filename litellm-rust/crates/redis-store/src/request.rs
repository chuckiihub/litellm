use std::{
    future::Future,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};

use futures_util::future::join_all;
use litellm_redis_client::{RedisBatch, RedisClient};
use tokio::{runtime::Handle, sync::oneshot};

/// One request's Redis batches: a pre-call batch for the reads it needs before answering and a
/// post-call batch for the writes nobody waits on, one of each per Redis client. The host that owns
/// the request builds one and passes it down; it knows nothing else about the request.
#[derive(Clone)]
pub struct RequestRedis(Arc<Shared>);

struct Shared {
    runtime: Handle,
    post_call_deadline: Duration,
    batches: Mutex<Vec<ClientBatches>>,
    deadline: Mutex<Option<oneshot::Sender<()>>>,
}

#[derive(Clone)]
struct ClientBatches {
    client: RedisClient,
    pre: RedisBatch,
    post: RedisBatch,
}

impl RequestRedis {
    /// `runtime` runs the post-call deadline, so declarations may come from any thread, including a
    /// Python one that has no Tokio context.
    pub fn new(runtime: Handle, post_call_deadline: Duration) -> Self {
        Self(Arc::new(Shared {
            runtime,
            post_call_deadline,
            batches: Mutex::new(Vec::new()),
            deadline: Mutex::new(None),
        }))
    }

    pub fn pre(&self, client: &RedisClient) -> RedisBatch {
        self.batches_for(client).pre
    }

    /// The post-call batch, which goes out on `flush_post` or `post_call_deadline` after the first
    /// declaration, whichever comes first.
    pub fn post(&self, client: &RedisClient) -> RedisBatch {
        self.arm_deadline();
        self.batches_for(client).post
    }

    /// Runs work that follows this request's batches, such as mirroring settled totals into
    /// memory, on the request's runtime, from any thread.
    pub fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) {
        self.0.runtime.spawn(task);
    }

    pub async fn flush_pre(&self) {
        join_all(self.batches().iter().map(|batches| batches.pre.flush())).await;
    }

    /// Sends every post-call op declared so far; the shutdown drain calls it too.
    pub async fn flush_post(&self) {
        lock(&self.0.deadline).take();
        join_all(self.batches().iter().map(|batches| batches.post.flush())).await;
    }

    fn batches(&self) -> Vec<ClientBatches> {
        lock(&self.0.batches).clone()
    }

    fn batches_for(&self, client: &RedisClient) -> ClientBatches {
        let mut batches = lock(&self.0.batches);
        if let Some(existing) = batches
            .iter()
            .find(|batches| batches.client.same_client(client))
        {
            return existing.clone();
        }
        let created = ClientBatches {
            client: client.clone(),
            pre: client.batch(),
            post: client.batch(),
        };
        batches.push(created.clone());
        created
    }

    fn arm_deadline(&self) {
        let mut deadline = lock(&self.0.deadline);
        if deadline.is_some() {
            return;
        }
        let (disarm, disarmed) = oneshot::channel();
        *deadline = Some(disarm);
        let request = self.clone();
        self.spawn(async move {
            tokio::select! {
                () = tokio::time::sleep(request.0.post_call_deadline) => request.flush_post().await,
                _ = disarmed => {}
            }
        });
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
