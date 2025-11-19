mod common;

use crate::common::build_random_items;
use chrono::Utc;
use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::time::Duration;
use dragonfly_playground_rs::redis_client::{build_mset_with_expire_pipeline, build_set_with_expiry_manual_pipeline, build_set_with_expiry_pipeline};

fn bench_pipeline_builders_10k(c: &mut Criterion) {
    let items = build_random_items(10_000, 80, 20);
    let ttl = Duration::from_secs(300);

    c.bench_function("build_mset_with_expire_pipeline_10k", |b| {
        b.iter(|| {
            let pipeline = build_mset_with_expire_pipeline(black_box(&items), Utc::now(), ttl);
            black_box(pipeline);
        });
    });

    c.bench_function("build_set_with_expiry_pipeline_10k", |b| {
        b.iter(|| {
            let pipeline = build_set_with_expiry_pipeline(black_box(&items), Utc::now(), ttl);
            black_box(pipeline);
        });
    });

    c.bench_function("build_set_with_expiry_manual_pipeline_10k", |b| {
        b.iter(|| {
            let pipeline =
                build_set_with_expiry_manual_pipeline(black_box(&items), Utc::now(), ttl);
            black_box(pipeline);
        });
    });
}

fn bench_pipeline_builders_100k(c: &mut Criterion) {
    let items = build_random_items(100_000, 80, 20);
    let ttl = Duration::from_secs(300);

    c.bench_function("build_mset_with_expire_pipeline_100k", |b| {
        b.iter(|| {
            let pipeline = build_mset_with_expire_pipeline(black_box(&items), Utc::now(), ttl);
            black_box(pipeline);
        });
    });

    c.bench_function("build_set_with_expiry_pipeline_100k", |b| {
        b.iter(|| {
            let pipeline = build_set_with_expiry_pipeline(black_box(&items), Utc::now(), ttl);
            black_box(pipeline);
        });
    });

    c.bench_function("build_set_with_expiry_manual_pipeline_100k", |b| {
        b.iter(|| {
            let pipeline =
                build_set_with_expiry_manual_pipeline(black_box(&items), Utc::now(), ttl);
            black_box(pipeline);
        });
    });
}

criterion_group!(
    pipeline_builder,
    bench_pipeline_builders_10k,
    bench_pipeline_builders_100k,
);
criterion_main!(pipeline_builder);
