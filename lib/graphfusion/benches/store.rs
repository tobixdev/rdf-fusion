#![allow(clippy::panic)]

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use futures::StreamExt;
use graphfusion::io::{RdfFormat, RdfParser};
use graphfusion::sparql::{Query, QueryOptions, QueryResults, Update};
use graphfusion::store::Store;
use oxhttp::model::{Method, Request, Status};
use rand::random;
use std::env::temp_dir;
use std::fs::{remove_dir_all, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::str;
use tokio::runtime::Runtime;

fn parse_nt(c: &mut Criterion) {
    let data = read_data("explore-1000.nt.zst");
    let mut group = c.benchmark_group("parse");
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.sample_size(50);
    group.bench_function("parse BSBM explore 1000", |b| {
        b.iter(|| {
            for r in RdfParser::from_format(RdfFormat::NTriples).for_slice(&data) {
                r.unwrap();
            }
        })
    });
    group.bench_function("parse BSBM explore 1000 with Read", |b| {
        b.iter(|| {
            for r in RdfParser::from_format(RdfFormat::NTriples).for_reader(data.as_slice()) {
                r.unwrap();
            }
        })
    });
    group.bench_function("parse BSBM explore 1000 unchecked", |b| {
        b.iter(|| {
            for r in RdfParser::from_format(RdfFormat::NTriples)
                .unchecked()
                .for_slice(&data)
            {
                r.unwrap();
            }
        })
    });
    group.bench_function("parse BSBM explore 1000 unchecked with Read", |b| {
        b.iter(|| {
            for r in RdfParser::from_format(RdfFormat::NTriples)
                .unchecked()
                .for_reader(data.as_slice())
            {
                r.unwrap();
            }
        })
    });
}

fn store_load(c: &mut Criterion) {
    let data = read_data("explore-1000.nt.zst");
    let mut group = c.benchmark_group("store load");
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.sample_size(10);
    group.bench_function("load BSBM explore 1000 in memory", |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| async {
            let store = Store::new().unwrap();
            do_load(&store, &data).await;
        });
    });
    // TODO #4: Persistent sotrage
    // group.bench_function("load BSBM explore 1000 in on disk", |b| {
    //     b.iter(|| {
    //         let path = TempDir::default();
    //         let store = Store::open(&path).unwrap();
    //         do_load(&store, &data);
    //     })
    // });
}

async fn do_load(store: &Store, data: &[u8]) {
    store
        .load_from_reader(RdfFormat::NTriples, data)
        .await
        .unwrap();
    // store.optimize().unwrap();
}

fn store_query_and_update(c: &mut Criterion) {
    let data = read_data("explore-1000.nt.zst");
    let operations = bsbm_sparql_operation()
        .into_iter()
        .map(|op| match op {
            RawOperation::Query(q) => Operation::Query(Query::parse(&q, None).unwrap()),
            RawOperation::Update(q) => Operation::Update(Update::parse(&q, None).unwrap()),
        })
        .collect::<Vec<_>>();
    let query_operations = operations
        .iter()
        .filter(|o| matches!(o, Operation::Query(_)))
        .cloned()
        .collect::<Vec<_>>();

    let mut group = c.benchmark_group("store operations");
    group.throughput(Throughput::Elements(operations.len() as u64));
    group.sample_size(10);

    let runtime = Runtime::new().unwrap();
    let memory_store = Store::new().unwrap();
    runtime.block_on(do_load(&memory_store, &data));
    group.bench_function("BSBM explore 1000 query in memory", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| run_operation(&memory_store, &query_operations, true))
    });
    group.bench_function(
        "BSBM explore 1000 query in memory without optimizations",
        |b| {
            b.to_async(Runtime::new().unwrap())
                .iter(|| run_operation(&memory_store, &query_operations, false))
        },
    );
    group.bench_function("BSBM explore 1000 queryAndUpdate in memory", |b| {
        b.to_async(Runtime::new().unwrap())
            .iter(|| run_operation(&memory_store, &operations, true))
    });
    group.bench_function(
        "BSBM explore 1000 queryAndUpdate in memory without optimizations",
        |b| {
            b.to_async(Runtime::new().unwrap())
                .iter(|| run_operation(&memory_store, &operations, false))
        },
    );

    // TODO #4: Persistent sotrage
    // {
    //     let path = TempDir::default();
    //     let disk_store = Store::open(&path).unwrap();
    //     do_load(&disk_store, &data);
    //     group.bench_function("BSBM explore 1000 query on disk", |b| {
    //         b.iter(|| run_operation(&disk_store, &query_operations, true))
    //     });
    //     group.bench_function(
    //         "BSBM explore 1000 query on disk without optimizations",
    //         |b| b.iter(|| run_operation(&disk_store, &query_operations, false)),
    //     );
    //     group.bench_function("BSBM explore 1000 queryAndUpdate on disk", |b| {
    //         b.iter(|| run_operation(&disk_store, &operations, true))
    //     });
    //     group.bench_function(
    //         "BSBM explore 1000 queryAndUpdate on disk without optimizations",
    //         |b| b.iter(|| run_operation(&disk_store, &operations, false)),
    //     );
    // }
}

async fn run_operation(store: &Store, operations: &[Operation], _with_opts: bool) {
    let options = QueryOptions;
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
            Operation::Update(u) => store.update_opt(u.clone(), options.clone()).unwrap(),
        }
    }
}

fn sparql_parsing(c: &mut Criterion) {
    let operations = bsbm_sparql_operation();
    let mut group = c.benchmark_group("sparql parsing");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(
        operations
            .iter()
            .map(|o| match o {
                RawOperation::Query(q) => q.len(),
                RawOperation::Update(u) => u.len(),
            })
            .sum::<usize>() as u64,
    ));
    group.bench_function("BSBM query and update set", |b| {
        b.iter(|| {
            for operation in &operations {
                match operation {
                    RawOperation::Query(q) => {
                        Query::parse(q, None).unwrap();
                    }
                    RawOperation::Update(u) => {
                        Update::parse(u, None).unwrap();
                    }
                }
            }
        })
    });
}

criterion_group!(parse, parse_nt);
criterion_group!(store, sparql_parsing, store_query_and_update, store_load);

criterion_main!(parse, store);

fn read_data(file: &str) -> Vec<u8> {
    if !Path::new(file).exists() {
        let client = oxhttp::Client::new().with_redirection_limit(5);
        let url = format!("https://github.com/Tpt/bsbm-tools/releases/download/v0.2/{file}");
        let request = Request::builder(Method::GET, url.parse().unwrap()).build();
        let response = client.request(request).unwrap();
        assert_eq!(
            response.status(),
            Status::OK,
            "{}",
            response.into_body().to_string().unwrap()
        );
        std::io::copy(&mut response.into_body(), &mut File::create(file).unwrap()).unwrap();
    }
    let mut buf = Vec::new();
    zstd::Decoder::new(File::open(file).unwrap())
        .unwrap()
        .read_to_end(&mut buf)
        .unwrap();
    buf
}

fn bsbm_sparql_operation() -> Vec<RawOperation> {
    String::from_utf8(read_data("mix-exploreAndUpdate-1000.tsv.zst"))
        .unwrap()
        .lines()
        .rev()
        .take(300) // We take only 10 groups
        .map(|l| {
            let mut parts = l.trim().split('\t');
            let kind = parts.next().unwrap();
            let operation = parts.next().unwrap();
            match kind {
                "query" => RawOperation::Query(operation.into()),
                "update" => RawOperation::Update(operation.into()),
                _ => panic!("Unexpected operation kind {kind}"),
            }
        })
        .collect()
}

#[derive(Clone)]
enum RawOperation {
    Query(String),
    Update(String),
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
enum Operation {
    Query(Query),
    Update(Update),
}

struct TempDir(PathBuf);

impl Default for TempDir {
    fn default() -> Self {
        Self(temp_dir().join(format!("graphfusion-bench-{}", random::<u128>())))
    }
}

impl AsRef<Path> for TempDir {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        remove_dir_all(&self.0).unwrap()
    }
}
