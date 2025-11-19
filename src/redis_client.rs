use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use futures::{StreamExt, stream};
use redis::aio::{ConnectionManager, MultiplexedConnection};
use redis::{
    AsyncCommands, ConnectionAddr, ConnectionInfo, Pipeline, ProtocolVersion, RedisConnectionInfo,
    RedisResult, SetExpiry, SetOptions, ToRedisArgs,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

pub trait AsyncRedisClient {
    fn ping(&self) -> impl Future<Output = RedisResult<String>>;

    fn multi_get(
        &self,
        keys: Vec<String>,
    ) -> impl Future<Output = RedisResult<Vec<Option<String>>>>;

    fn multi_set<K: ToRedisArgs + Sync + Send, V: ToRedisArgs + Sync + Send>(
        &self,
        items: &[(K, V)],
    ) -> impl Future<Output = RedisResult<()>>;

    fn pipelined_multi_set_with_expiry<K: ToRedisArgs + Sync + Send, V: ToRedisArgs + Sync + Send>(
        &self,
        items: &[(K, V)],
        ttl: Duration,
    ) -> impl Future<Output = RedisResult<()>>;

    fn pipelined_set_with_expiry<K: ToRedisArgs + Sync + Send, V: ToRedisArgs + Sync + Send>(
        &self,
        items: &[(K, V)],
        ttl: Duration,
    ) -> impl Future<Output = RedisResult<()>>;

    fn pipelined_set_with_expiry_manual<
        K: ToRedisArgs + Sync + Send,
        V: ToRedisArgs + Sync + Send,
    >(
        &self,
        items: &[(K, V)],
        ttl: Duration,
    ) -> impl Future<Output = RedisResult<()>>;

    fn server_adder(&self) -> String;
}

pub struct AsyncRedisClientV1 {
    pub conn_info: ConnectionInfo,
    pub conn: ConnectionManager,
    batch_size: usize,
}

impl AsyncRedisClientV1 {
    pub async fn new(conn_info: ConnectionInfo, batch_size: usize) -> RedisResult<Self> {
        let client = redis::Client::open(conn_info.clone())?;
        let conn = ConnectionManager::new(client).await?;
        Ok(Self {
            conn_info,
            conn,
            batch_size,
        })
    }
}
impl AsyncRedisClient for AsyncRedisClientV1 {
    async fn ping(&self) -> RedisResult<String> {
        self.conn.clone().ping().await
    }

    async fn multi_get(&self, keys: Vec<String>) -> RedisResult<Vec<Option<String>>> {
        self.conn.clone().mget(keys).await
    }

    async fn multi_set<K: ToRedisArgs + Sync + Send, V: ToRedisArgs + Sync + Send>(
        &self,
        items: &[(K, V)],
    ) -> RedisResult<()> {
        if items.len() > self.batch_size {
            for chunk in items.chunks(self.batch_size) {
                self.conn
                    .clone()
                    .mset::<K, V, ()>(chunk)
                    .await
                    .map_err(|e| {
                        info!("Failed to sync {} features: {}", chunk.len(), e);
                        e
                    })?;
            }
            Ok(())
        } else {
            self.conn.clone().mset::<K, V, ()>(items).await
        }
    }

    async fn pipelined_multi_set_with_expiry<
        K: ToRedisArgs + Sync + Send,
        V: ToRedisArgs + Sync + Send,
    >(
        &self,
        items: &[(K, V)],
        ttl: Duration,
    ) -> RedisResult<()> {
        for chunk in items.chunks(self.batch_size) {
            debug!("Executing mset+expire pipeline with {} items", chunk.len());
            let now = Instant::now();

            let pipeline = build_mset_with_expire_pipeline(chunk, Utc::now(), ttl);

            let mut conn = self.conn.clone();
            pipeline.exec_async(&mut conn).await.map_err(|e| {
                warn!("Failed to sync {} features: {}", chunk.len(), e);
                e
            })?;
            debug!(
                "Executed pipeline with {} items in {} ms",
                chunk.len(),
                now.elapsed().as_millis()
            );
        }
        Ok(())
    }

    async fn pipelined_set_with_expiry<
        K: ToRedisArgs + Sync + Send,
        V: ToRedisArgs + Sync + Send,
    >(
        &self,
        items: &[(K, V)],
        ttl: Duration,
    ) -> RedisResult<()> {
        for chunk in items.chunks(self.batch_size) {
            debug!("Executing per-key set pipeline with {} items", chunk.len());
            let now = Instant::now();
            let pipeline = build_set_with_expiry_pipeline(chunk, Utc::now(), ttl);

            let mut conn = self.conn.clone();
            pipeline.exec_async(&mut conn).await.map_err(|e| {
                warn!("Failed to sync {} features: {}", chunk.len(), e);
                e
            })?;
            debug!(
                "Executed pipeline with {} items in {} ms",
                chunk.len(),
                now.elapsed().as_millis()
            );
        }
        Ok(())
    }

    async fn pipelined_set_with_expiry_manual<
        K: ToRedisArgs + Sync + Send,
        V: ToRedisArgs + Sync + Send,
    >(
        &self,
        items: &[(K, V)],
        ttl: Duration,
    ) -> RedisResult<()> {
        for chunk in items.chunks(self.batch_size) {
            debug!("Executing per-key set pipeline with {} items", chunk.len());
            let now = Instant::now();
            let pipeline = build_set_with_expiry_manual_pipeline(chunk, Utc::now(), ttl);

            let mut conn = self.conn.clone();
            pipeline.exec_async(&mut conn).await.map_err(|e| {
                warn!("Failed to sync {} features: {}", chunk.len(), e);
                e
            })?;
            debug!(
                "Executed pipeline with {} items in {} ms",
                chunk.len(),
                now.elapsed().as_millis()
            );
        }
        Ok(())
    }

    fn server_adder(&self) -> String {
        self.conn_info.addr.to_string()
    }
}

struct ChunkCommand {
    len: usize,
    cmd: redis::Cmd,
}

pub struct AsyncRedisClientPooled {
    pub conn_info: ConnectionInfo,
    pub conn: ConnectionManager,
    batch_size: usize,
    write_parallelism: usize,
    write_connection_pool_size: usize,
    write_connections: Vec<Mutex<MultiplexedConnection>>,
    next_id: AtomicUsize,
}

impl AsyncRedisClientPooled {
    pub async fn new(
        conn_info: ConnectionInfo,
        batch_size: usize,
        write_parallelism: usize,
        write_connection_pool_size: usize,
    ) -> RedisResult<Self> {
        let parallelism = write_parallelism.max(1);
        let pool_size = write_connection_pool_size.max(parallelism).max(1);
        let client = redis::Client::open(conn_info.clone())?;
        let conn = ConnectionManager::new(client.clone()).await?;
        let mut pooled_conns = Vec::with_capacity(pool_size);

        for _ in 0..pool_size {
            let connection = client.get_multiplexed_async_connection().await?;
            pooled_conns.push(Mutex::new(connection));
        }

        Ok(Self {
            conn_info,
            conn,
            batch_size,
            write_parallelism: parallelism,
            write_connection_pool_size: pool_size,
            write_connections: pooled_conns,
            next_id: AtomicUsize::new(0),
        })
    }

    async fn with_next_connection<T, F>(&self, execute_fn: F) -> RedisResult<T>
    where
        for<'a> F: FnOnce(&'a mut MultiplexedConnection) -> BoxFuture<'a, RedisResult<T>>,
    {
        let start = self.next_id.fetch_add(1, Ordering::SeqCst);
        let pool_size = self.write_connection_pool_size;

        for offset in 0..pool_size {
            let idx = (start + offset) % pool_size;
            if let Ok(mut conn_guard) = self.write_connections[idx].try_lock() {
                return execute_fn(&mut conn_guard).await;
            }
        }

        let idx = start % pool_size;
        let mut conn_guard = self.write_connections[idx].lock().await;
        execute_fn(&mut conn_guard).await
    }

    async fn execute_pipelines(
        &self,
        pipelines: Vec<(usize, Pipeline)>,
        context: &'static str,
    ) -> RedisResult<()> {
        if pipelines.is_empty() {
            return Ok(());
        }

        let mut tasks = stream::iter(pipelines.into_iter().map(
            move |(chunk_len, pipeline)| async move {
                let started = Instant::now();
                let result = self
                    .with_next_connection(move |conn| {
                        Box::pin(async move { pipeline.exec_async(conn).await })
                    })
                    .await;
                (chunk_len, started, result)
            },
        ))
        .buffer_unordered(self.write_parallelism);

        let mut first_error: Option<redis::RedisError> = None;

        while let Some((chunk_len, started, result)) = tasks.next().await {
            match result {
                Ok(()) => {
                    debug!(
                        "Executed pipeline with {} items in {} ms",
                        chunk_len,
                        started.elapsed().as_millis()
                    );
                }
                Err(err) => {
                    warn!(
                        "Failed to sync {} features via {}: {}",
                        chunk_len, context, err
                    );
                    if first_error.is_none() {
                        first_error = Some(err);
                    }
                }
            }
        }

        if let Some(err) = first_error {
            Err(err)
        } else {
            Ok(())
        }
    }
}

impl AsyncRedisClient for AsyncRedisClientPooled {
    async fn ping(&self) -> RedisResult<String> {
        self.conn.clone().ping().await
    }

    async fn multi_get(&self, keys: Vec<String>) -> RedisResult<Vec<Option<String>>> {
        self.conn.clone().mget(keys).await
    }

    async fn multi_set<K: ToRedisArgs + Sync + Send, V: ToRedisArgs + Sync + Send>(
        &self,
        items: &[(K, V)],
    ) -> RedisResult<()> {
        if items.is_empty() {
            return Ok(());
        }

        let commands: Vec<ChunkCommand> = items
            .chunks(self.batch_size)
            .map(|chunk| {
                let mut cmd = redis::cmd("MSET");
                for (k, v) in chunk {
                    cmd.arg(k);
                    cmd.arg(v);
                }
                ChunkCommand {
                    len: chunk.len(),
                    cmd,
                }
            })
            .collect();

        let mut tasks = stream::iter(commands.into_iter().map(move |chunk| async move {
            let ChunkCommand {
                len: chunk_len,
                cmd,
            } = chunk;
            let result = self
                .with_next_connection(move |conn| {
                    Box::pin(async move { cmd.query_async::<()>(conn).await })
                })
                .await;
            (chunk_len, result)
        }))
        .buffer_unordered(self.write_parallelism);

        let mut first_error: Option<redis::RedisError> = None;

        while let Some((chunk_len, result)) = tasks.next().await {
            if let Err(err) = result {
                info!("Failed to sync {} features: {}", chunk_len, err);
                if first_error.is_none() {
                    first_error = Some(err);
                }
            }
        }

        if let Some(err) = first_error {
            Err(err)
        } else {
            Ok(())
        }
    }

    async fn pipelined_multi_set_with_expiry<
        K: ToRedisArgs + Sync + Send,
        V: ToRedisArgs + Sync + Send,
    >(
        &self,
        items: &[(K, V)],
        ttl: Duration,
    ) -> RedisResult<()> {
        let pipelines: Vec<_> = items
            .chunks(self.batch_size)
            .map(|chunk| {
                debug!("Executing mset+expire pipeline with {} items", chunk.len());
                let pipeline = build_mset_with_expire_pipeline(chunk, Utc::now(), ttl);
                (chunk.len(), pipeline)
            })
            .collect();

        self.execute_pipelines(pipelines, "mset+expire").await
    }

    async fn pipelined_set_with_expiry<
        K: ToRedisArgs + Sync + Send,
        V: ToRedisArgs + Sync + Send,
    >(
        &self,
        items: &[(K, V)],
        ttl: Duration,
    ) -> RedisResult<()> {
        let pipelines: Vec<_> = items
            .chunks(self.batch_size)
            .map(|chunk| {
                debug!("Executing per-key set pipeline with {} items", chunk.len());
                let pipeline = build_set_with_expiry_pipeline(chunk, Utc::now(), ttl);
                (chunk.len(), pipeline)
            })
            .collect();

        self.execute_pipelines(pipelines, "set+expiry").await
    }

    async fn pipelined_set_with_expiry_manual<
        K: ToRedisArgs + Sync + Send,
        V: ToRedisArgs + Sync + Send,
    >(
        &self,
        items: &[(K, V)],
        ttl: Duration,
    ) -> RedisResult<()> {
        let pipelines: Vec<_> = items
            .chunks(self.batch_size)
            .map(|chunk| {
                debug!("Executing per-key set pipeline with {} items", chunk.len());
                let pipeline = build_set_with_expiry_manual_pipeline(chunk, Utc::now(), ttl);
                (chunk.len(), pipeline)
            })
            .collect();

        self.execute_pipelines(pipelines, "manual set+expiry").await
    }

    fn server_adder(&self) -> String {
        self.conn_info.addr.to_string()
    }
}

/// Builds a pipeline that performs `MSET` followed by individual `EXPIREAT` commands.
pub fn build_mset_with_expire_pipeline<
    K: ToRedisArgs + Sync + Send,
    V: ToRedisArgs + Sync + Send,
>(
    chunk: &[(K, V)],
    now: DateTime<Utc>,
    ttl: Duration,
) -> Pipeline {
    let mut pipeline = redis::pipe();
    pipeline.mset(chunk);
    let expiry_ts = (now + ttl).timestamp();
    for (k, _) in chunk {
        pipeline.expire_at(k, expiry_ts);
    }
    pipeline
}

/// Builds a pipeline that issues `SET key value PX ttl` per entry.
pub fn build_set_with_expiry_pipeline<
    K: ToRedisArgs + Sync + Send,
    V: ToRedisArgs + Sync + Send,
>(
    chunk: &[(K, V)],
    now: DateTime<Utc>,
    ttl: Duration,
) -> Pipeline {
    let mut pipeline = redis::pipe();
    let expiry_ts = (now + ttl).timestamp();
    for (k, v) in chunk {
        let opts = SetOptions::default().with_expiration(SetExpiry::EXAT(expiry_ts as u64));
        pipeline.set_options(k, v, opts);
    }
    pipeline
}

/// Builds a pipeline that issues `SET key value PX ttl` per entry.
pub fn build_set_with_expiry_manual_pipeline<
    K: ToRedisArgs + Sync + Send,
    V: ToRedisArgs + Sync + Send,
>(
    chunk: &[(K, V)],
    now: DateTime<Utc>,
    ttl: Duration,
) -> Pipeline {
    let mut pipeline = redis::pipe();
    let expiry_ts = (now + ttl).timestamp();
    for (k, v) in chunk {
        pipeline.cmd("SET").arg(k).arg(v).arg("EXAT").arg(expiry_ts);
    }
    pipeline
}

pub fn get_connection_info(
    server: String,
    database_slot: i64,
    protocol_version: ProtocolVersion,
    username: Option<String>,
    passwd: Option<String>,
) -> ConnectionInfo {
    let conn_addr = {
        let s = server.split(":").collect::<Vec<&str>>();
        if s.len() == 2 {
            ConnectionAddr::Tcp(s[0].to_string(), s[1].parse().unwrap())
        } else {
            ConnectionAddr::Tcp(server.clone(), 6379)
        }
    };

    let conn = ConnectionInfo {
        addr: conn_addr,
        redis: RedisConnectionInfo {
            db: database_slot,
            username: username,
            password: passwd,
            protocol: protocol_version,
        },
    };
    conn
}

#[derive(Clone)]
pub struct RedisClientFactory {
    pub conn_info: ConnectionInfo,
    pub batch_size: usize,
    pub write_parallelism: usize,
    pub write_connection_pool_size: usize,
}

impl RedisClientFactory {
    pub async fn create(&self) -> RedisResult<AsyncRedisClientPooled> {
        AsyncRedisClientPooled::new(
            self.conn_info.clone(),
            self.batch_size,
            self.write_parallelism,
            self.write_connection_pool_size,
        )
        .await
    }
}
