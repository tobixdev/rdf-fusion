//! Runs the queries from the Wind Farm Benchmark.

use codspeed_criterion_compat::{criterion_group, criterion_main, Criterion};
use futures::StreamExt;
use rdf_fusion::io::RdfFormat;
use rdf_fusion::store::Store;
use rdf_fusion::{QueryOptions, QueryResults};
use std::fs;
use std::path::PathBuf;
use tokio::runtime::{Builder, Runtime};

fn wind_farm_grouped_production(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_wind_farm_1000()).unwrap();

    c.bench_function("Wind Farm 500 - Grouped Production 1", |b| {
        b.to_async(&runtime).iter(|| async {
            benchmark_query(&store, "grouped_production_query1.sparql").await;
        });
    });

    c.bench_function("Wind Farm 500 - Grouped Production 2", |b| {
        b.to_async(&runtime).iter(|| async {
            benchmark_query(&store, "grouped_production_query2.sparql").await;
        });
    });

    c.bench_function("Wind Farm 500 - Grouped Production 3", |b| {
        b.to_async(&runtime).iter(|| async {
            benchmark_query(&store, "grouped_production_query3.sparql").await;
        });
    });

    c.bench_function("Wind Farm 500 - Grouped Production 4", |b| {
        b.to_async(&runtime).iter(|| async {
            benchmark_query(&store, "grouped_production_query4.sparql").await;
        });
    });
}

fn wind_farm_multi_grouped(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_wind_farm_1000()).unwrap();

    c.bench_function("Wind Farm 500 - Multi Grouped 1", |b| {
        b.to_async(&runtime).iter(|| async {
            benchmark_query(&store, "multi_grouped_query1.sparql").await;
        });
    });

    c.bench_function("Wind Farm 500 - Multi Grouped 2", |b| {
        b.to_async(&runtime).iter(|| async {
            benchmark_query(&store, "multi_grouped_query2.sparql").await;
        });
    });

    c.bench_function("Wind Farm 500 - Multi Grouped 3", |b| {
        b.to_async(&runtime).iter(|| async {
            benchmark_query(&store, "multi_grouped_query3.sparql").await;
        });
    });

    c.bench_function("Wind Farm 500 - Multi Grouped 4", |b| {
        b.to_async(&runtime).iter(|| async {
            benchmark_query(&store, "multi_grouped_query4.sparql").await;
        });
    });
}

fn wind_farm_production(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_wind_farm_1000()).unwrap();

    c.bench_function("Wind Farm 500 - Production 1", |b| {
        b.to_async(&runtime).iter(|| async {
            benchmark_query(&store, "production_query1.sparql").await;
        });
    });

    c.bench_function("Wind Farm 500 - Production 2", |b| {
        b.to_async(&runtime).iter(|| async {
            benchmark_query(&store, "production_query2.sparql").await;
        });
    });

    c.bench_function("Wind Farm 500 - Production 3", |b| {
        b.to_async(&runtime).iter(|| async {
            benchmark_query(&store, "production_query3.sparql").await;
        });
    });

    c.bench_function("Wind Farm 500 - Production 4", |b| {
        b.to_async(&runtime).iter(|| async {
            benchmark_query(&store, "production_query4.sparql").await;
        });
    });
}

criterion_group!(
    wind_farm,
    wind_farm_grouped_production,
    wind_farm_multi_grouped,
    wind_farm_production,
);
criterion_main!(wind_farm);

fn create_runtime() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

async fn benchmark_query(store: &Store, query_path: &str) {
    let query_path =
        PathBuf::from("./data/wind-farm-500/benchmark-docker/queries_chrontext/")
            .join(query_path);
    let query = fs::read_to_string(query_path).unwrap();
    let result = store
        .query_opt(query, QueryOptions::default())
        .await
        .unwrap();
    assert_number_of_results(result, 0).await;
}

async fn load_wind_farm_1000() -> anyhow::Result<Store> {
    let data_path = PathBuf::from("./data/wind-farm-500/dataset.nt");
    let data = fs::read(data_path)?;
    let memory_store = Store::new();
    memory_store
        .load_from_reader(RdfFormat::NTriples, data.as_slice())
        .await?;
    Ok(memory_store)
}

async fn assert_number_of_results(result: QueryResults, n: usize) {
    match result {
        QueryResults::Solutions(mut solutions) => {
            let mut count = 0;
            while let Some(sol) = solutions.next().await {
                sol.unwrap();
                count += 1;
            }
            assert_eq!(count, n);
        }
        QueryResults::Graph(mut triples) => {
            let mut count = 0;
            while let Some(sol) = triples.next().await {
                sol.unwrap();
                count += 1;
            }
            assert_eq!(count, n);
        }
        _ => panic!("Unexpected QueryResults"),
    }
}
