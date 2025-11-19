use rand::Rng;
use rand::distr::Alphanumeric;
use redis::{ConnectionInfo, ProtocolVersion, RedisResult, ToRedisArgs};
use std::env;
use std::time::Duration;
use dragonfly_playground_rs::redis_client::{
    AsyncRedisClient, AsyncRedisClientPooled, AsyncRedisClientV1, get_connection_info,
};

pub fn build_random_items(
    count: usize,
    key_size: usize,
    value_size: usize,
) -> Vec<(String, Vec<u8>)> {
    let mut rng = rand::rng();
    (0..count)
        .map(|_| {
            let key: String = (&mut rng)
                .sample_iter(Alphanumeric)
                .take(key_size)
                .map(char::from)
                .collect();

            let mut value = vec![0u8; value_size];
            rng.fill(&mut value[..]);
            (key, value)
        })
        .collect() // Collect all pairs into the final Vec
}

pub enum BenchRedisClient {
    V1(AsyncRedisClientV1),
    Pooled(AsyncRedisClientPooled),
}
impl BenchRedisClient {
    pub async fn multi_set<K, V>(&self, items: &[(K, V)]) -> RedisResult<()>
    where
        K: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + Send + Sync,
    {
        match self {
            BenchRedisClient::V1(c) => c.multi_set(items).await,
            BenchRedisClient::Pooled(c) => c.multi_set(items).await,
        }
    }

    pub async fn pipelined_multi_set_with_expiry<K, V>(
        &self,
        items: &[(K, V)],
        ttl: Duration,
    ) -> RedisResult<()>
    where
        K: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + Send + Sync,
    {
        match self {
            BenchRedisClient::V1(c) => c.pipelined_multi_set_with_expiry(items, ttl).await,
            BenchRedisClient::Pooled(c) => c.pipelined_multi_set_with_expiry(items, ttl).await,
        }
    }

    pub async fn pipelined_set_with_expiry<K, V>(
        &self,
        items: &[(K, V)],
        ttl: Duration,
    ) -> RedisResult<()>
    where
        K: ToRedisArgs + Send + Sync,
        V: ToRedisArgs + Send + Sync,
    {
        match self {
            BenchRedisClient::V1(c) => c.pipelined_set_with_expiry(items, ttl).await,
            BenchRedisClient::Pooled(c) => c.pipelined_set_with_expiry(items, ttl).await,
        }
    }

    pub async fn pipelined_set_with_expiry_manual<
        K: ToRedisArgs + Sync + Send,
        V: ToRedisArgs + Sync + Send,
    >(
        &self,
        items: &[(K, V)],
        ttl: Duration,
    ) -> RedisResult<()> {
        match self {
            BenchRedisClient::V1(c) => c.pipelined_set_with_expiry_manual(items, ttl).await,
            BenchRedisClient::Pooled(c) => c.pipelined_set_with_expiry_manual(items, ttl).await,
        }
    }
}

#[derive(Debug, Clone)]
pub enum ClientType {
    AsyncRedisClientV1 {
        connection_info: ConnectionInfo,
        batch_size: usize,
    },
    AsyncRedisClientPooled {
        connection_info: ConnectionInfo,
        batch_size: usize,
        write_parallelism: usize,
        write_connection_pool_size: usize,
    },
}

impl ClientType {
    pub fn name(&self) -> String {
        match self {
            ClientType::AsyncRedisClientV1 { .. } => "AsyncRedisClientV1".to_string(),
            ClientType::AsyncRedisClientPooled { .. } => " AsyncRedisClientPooled".to_string(),
        }
    }
}

pub fn create_connection_info() -> ConnectionInfo {
    let Some(server) = env::var("REDIS_BENCH_SERVER").ok() else {
        eprintln!("Skipping redis_end_to_end latency benchmark: REDIS_BENCH_SERVER not set");
        panic!("REDIS_BENCH_SERVER not set");
    };

    let username: Option<String> = env::var("REDIS_BENCH_USERNAME").ok();
    let password: Option<String> = env::var("REDIS_BENCH_PASSWORD").ok();
    let database_slot: i64 = env::var("REDIS_BENCH_DB")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);

    let connection_info = get_connection_info(
        server.clone(),
        database_slot,
        ProtocolVersion::RESP3,
        username.clone(),
        password.clone(),
    );
    connection_info
}

pub fn get_v1_client_type(batch_size: usize) -> ClientType {
    let connection_info = create_connection_info();
    ClientType::AsyncRedisClientV1 {
        connection_info,
        batch_size,
    }
}

pub fn get_pooled_client_type(
    batch_size: usize,
    write_parallelism: usize,
    write_connection_pool_size: usize,
) -> ClientType {
    let connection_info = create_connection_info();
    ClientType::AsyncRedisClientPooled {
        connection_info,
        batch_size,
        write_parallelism,
        write_connection_pool_size,
    }
}

pub fn show_info(client_name: &String, client_cfg: String, items: &Vec<(String, Vec<u8>)>) {
    let total_items: usize = items.len();
    let total_size_of_key = items.iter().map(|(k, _)| k.len()).sum::<usize>();
    println!("Total number of items: {}", items.len());
    println!("Using {} with params: {client_cfg}", client_name);
    println!(
        "Total length of keys: {}, average len: {} ",
        total_size_of_key,
        total_size_of_key as f64 / total_items as f64
    );

    let total_size_of_key_bytes = items.iter().map(|(k, _)| k.as_bytes().len()).sum::<usize>();
    println!(
        "Total size of keys: {} bytes, average size: {} bytes",
        total_size_of_key_bytes,
        total_size_of_key_bytes as f64 / total_items as f64
    );

    let total_size_of_value_bytes = items.iter().map(|(_, v)| v.len()).sum::<usize>();
    println!(
        "Total size of values: {} bytes, average size: {} bytes",
        total_size_of_value_bytes,
        total_size_of_value_bytes as f64 / total_items as f64
    );

    let total_size = total_size_of_key_bytes + total_size_of_value_bytes;
    println!(
        "Total size of items: {} bytes, average size: {} bytes",
        total_size,
        total_size as f64 / total_items as f64
    );
}
