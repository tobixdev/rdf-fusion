#![allow(clippy::panic)]

use codspeed_criterion_compat::{
    criterion_group, criterion_main, BenchmarkId, Criterion, Throughput,
};
use futures::StreamExt;
use oxrdfio::RdfFormat;
use rdf_fusion::store::Store;
use rdf_fusion_engine::results::QueryResults;
use rdf_fusion_engine::sparql::{Query, QueryOptions};
use std::{fs, str};
use tokio::runtime::{Builder, Runtime};

fn store_load(c: &mut Criterion) {
    let data = read_benchmark_file("dataset-1000.nt");
    let mut group = c.benchmark_group("Store::load");
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.bench_function(BenchmarkId::new("Load BSBM explore in memory", 1000), |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| async {
            let store = Store::new();
            do_load(&store, &data).await;
        });
    });
}

async fn do_load(store: &Store, data: &[u8]) {
    store
        .load_from_reader(RdfFormat::NTriples, data)
        .await
        .unwrap();
}

fn store_bsbm_explore(c: &mut Criterion) {
    for data_size in [1_000, 5_000] {
        for num_threads in [1, 2, 4] {
            execute_bsbm_explore(c, num_threads, data_size)
        }
    }
}

fn execute_bsbm_explore(c: &mut Criterion, num_threads: usize, data_size: usize) {
    let runtime = Builder::new_multi_thread()
        .worker_threads(num_threads)
        .enable_all()
        .build()
        .expect("Could not build benchmark runtime");
    let data = read_benchmark_file(&format!("dataset-{data_size}.nt"));

    let explore_queries = bsbm_sparql_operation("exploreAndUpdate-1000.csv")
        .into_iter()
        .filter_map(|op| match op {
            RawOperation::Query(q) => {
                // TODO remove once describe is supported
                if q.contains("DESCRIBE") {
                    None
                } else {
                    Some(Operation::Query(Query::parse(&q, None).unwrap()))
                }
            }
            RawOperation::Update(_) => None,
        })
        .collect::<Vec<_>>();

    let mut group = c.benchmark_group("BSBM Explore");
    group.sample_size(10);

    {
        let memory_store = Store::new();
        runtime.block_on(do_load(&memory_store, &data));

        group.bench_function(
            BenchmarkId::new(
                "Query",
                format!("num_threads={num_threads}, data_size={data_size}"),
            ),
            |b| {
                b.to_async(&runtime)
                    .iter(|| run_operation(&memory_store, &explore_queries));
            },
        );
    }
}

async fn run_operation(store: &Store, operations: &[Operation]) {
    let options = QueryOptions::default();
    for operation in operations {
        match operation {
            Operation::Query(q) => match store.query_opt(q.clone(), options.clone()).await.unwrap()
            {
                QueryResults::Boolean(_) => (),
                QueryResults::Solutions(mut s) => {
                    while let Some(s) = s.next().await {
                        s.unwrap();
                    }
                }
                QueryResults::Graph(mut g) => {
                    while let Some(t) = g.next().await {
                        t.unwrap();
                    }
                }
            },
            // Operation::Update(u) => store.update_opt(u.clone(), options.clone()).await.unwrap(),
        }
    }
}

criterion_group!(store_write, store_load);
criterion_group!(store_bsbm, store_bsbm_explore);
criterion_main!(store_write, store_bsbm);

fn bsbm_sparql_operation(file_name: &str) -> Vec<RawOperation> {
    csv::Reader::from_reader(read_benchmark_file(file_name).as_slice()).records()
        .collect::<Result<Vec<_>, _>>().unwrap()
        .into_iter()
        .rev()
        .take(10) // We take only 10 groups // TODO Increase to 300
        .map(|l| {
            match &l[1] {
                "query" => RawOperation::Query(l[2].into()),
                "update" => RawOperation::Update(l[2].into()),
                _ => panic!("Unexpected operation kind {}", &l[1]),
            }
        })
        .collect()
}

fn read_benchmark_file(file_name: &str) -> Vec<u8> {
    let file_name = format!("./benches-data/{file_name}");
    fs::read(&file_name)
        .expect(&format!("Did not find file '{file_name}') in the benches-data. Did you download the benchmark datasets?"))
}

#[allow(dead_code)]
#[derive(Clone)]
enum RawOperation {
    Query(String),
    Update(String),
}

#[allow(clippy::large_enum_variant, clippy::allow_attributes)]
#[derive(Clone)]
enum Operation {
    Query(Query),
}
