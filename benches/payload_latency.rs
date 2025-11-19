mod common;

use crate::common::{
    BenchRedisClient, ClientType, build_random_items,
    get_v1_client_type, show_info,
};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use dragonfly_playground_rs::redis_client::{AsyncRedisClientPooled, AsyncRedisClientV1};

fn get_total_items() -> usize {
    let batch_size = env::var("REDIS_BENCH_TOTAL_ITEMS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(100_000);
    batch_size
}

fn get_batch_size() -> usize {
    let batch_size = env::var("REDIS_BENCH_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(10_000);
    batch_size
}

fn e2e_8_8(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 8, 8);
}

fn e2e_16_8(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 16, 8);
}

fn e2e_32_8(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 32, 8);
}

fn e2e_64_8(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 64, 8);
}

fn e2e_128_8(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 128, 8);
}

fn e2e_8_16(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 8, 16);
}

fn e2e_16_16(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 16, 16);
}

fn e2e_32_16(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 32, 16);
}

fn e2e_64_16(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 64, 16);
}

fn e2e_128_16(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 128, 16);
}

fn e2e_8_32(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 8, 32);
}

fn e2e_16_32(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 16, 32);
}

fn e2e_32_32(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 32, 32);
}

fn e2e_64_32(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 64, 32);
}

fn e2e_128_32(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 128, 32);
}

fn e2e_8_64(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 8, 64);
}

fn e2e_16_64(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 16, 64);
}

fn e2e_32_64(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 32, 64);
}

fn e2e_64_64(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 64, 64);
}

fn e2e_128_64(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 128, 64);
}

fn e2e_8_128(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 8, 128);
}

fn e2e_16_128(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 16, 128);
}

fn e2e_32_128(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 32, 128);
}

fn e2e_64_128(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 64, 128);
}

fn e2e_128_128(c: &mut Criterion) {
    let total_items = get_total_items();
    let batch_size = get_batch_size();
    let client_type = get_v1_client_type(batch_size);
    bench_end_to_end_latency(c, client_type, total_items, 128, 128);
}

fn bench_end_to_end_latency(
    c: &mut Criterion,
    client_type: ClientType,
    total_items: usize,
    key_size: usize,
    value_size: usize,
) {
    let ttl_secs = env::var("REDIS_BENCH_TTL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(300);

    let rt = Runtime::new().expect("tokio runtime for benchmarks");

    let clien_name = client_type.name();

    let (client, client_cfg) = match client_type {
        ClientType::AsyncRedisClientV1 {
            connection_info,
            batch_size,
        } => {
            let c = rt
                .block_on(AsyncRedisClientV1::new(connection_info, batch_size))
                .expect("Unable to initialize AsyncRedisClientPooled");
            (BenchRedisClient::V1(c), format!("batch_size: {batch_size}"))
        }
        ClientType::AsyncRedisClientPooled {
            connection_info,
            batch_size,
            write_parallelism,
            write_connection_pool_size,
        } => {
            let c = rt
                .block_on(AsyncRedisClientPooled::new(
                    connection_info,
                    batch_size,
                    write_parallelism,
                    write_connection_pool_size,
                ))
                .expect("Unable to initialize AsyncRedisClientPooled");
            (
                BenchRedisClient::Pooled(c),
                format!(
                    "batch_size: {batch_size}, write_parallelism: {write_parallelism}, write_connection_pool_size: {write_connection_pool_size}"
                ),
            )
        }
    };

    let client = Arc::new(client);

    let ttl = Duration::from_secs(ttl_secs);
    let items = build_random_items(total_items, key_size, value_size);

    show_info(&clien_name, client_cfg, &items);

    let items = Arc::new(items);

    let group_name = format!(
        "{} key size {}, value size {}, {}k items",
        clien_name,
        key_size,
        value_size,
        total_items / 1000,
    );

    let mut group = c.benchmark_group(group_name);
    group.measurement_time(Duration::from_secs(60));
    group.warm_up_time(Duration::from_secs(5));
    group.throughput(Throughput::Elements(items.len() as u64));

    let is_multi_set = env::var("REDIS_BENCH_MULTI_SET").ok().is_some();

    if is_multi_set {
        group.bench_function("multi_set", |b| {
            let client = client.clone();
            let items = items.clone();
            b.to_async(&rt).iter(|| async {
                client.multi_set(items.as_slice()).await.expect("multi_set");
            });
        });
    } else {
        group.bench_function("pipelined_set_with_expiry_manual", |b| {
            let client = client.clone();
            let items = items.clone();
            b.to_async(&rt).iter(|| async {
                client
                    .pipelined_set_with_expiry_manual(items.as_slice(), ttl)
                    .await
                    .expect("pipelined set expire manual write");
            });
        });
    }

    group.finish();
}

criterion_group!(
    e2e,
    e2e_8_8,
    e2e_16_8,
    e2e_32_8,
    e2e_64_8,
    e2e_128_8,
    e2e_8_16,
    e2e_16_16,
    e2e_32_16,
    e2e_64_16,
    e2e_128_16,
    e2e_8_32,
    e2e_16_32,
    e2e_32_32,
    e2e_64_32,
    e2e_128_32,
    e2e_8_64,
    e2e_16_64,
    e2e_32_64,
    e2e_64_64,
    e2e_128_64,
    e2e_8_128,
    e2e_16_128,
    e2e_32_128,
    e2e_64_128,
    e2e_128_128,
);
criterion_main!(e2e);
