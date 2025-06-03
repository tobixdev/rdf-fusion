#![allow(clippy::panic)]

use bzip2::read::MultiBzDecoder;
use codspeed_criterion_compat::{criterion_group, criterion_main, Criterion, Throughput};
use futures::StreamExt;
use oxrdfio::RdfFormat;
use rdf_fusion::store::Store;
use rdf_fusion_engine::results::QueryResults;
use rdf_fusion_engine::sparql::{Query, QueryOptions};
use reqwest::Url;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::str;
use std::str::FromStr;
use tokio::runtime::Runtime;

fn store_load(c: &mut Criterion) {
    let data = read_bz2_data("https://zenodo.org/records/12663333/files/dataset-1000.nt.bz2");
    let mut group = c.benchmark_group("store load");
    group.throughput(Throughput::Bytes(data.len() as u64));
    group.sample_size(10);
    group.bench_function("load BSBM explore 1000 in memory", |b| {
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

fn store_query_and_update(c: &mut Criterion) {
    for data_size in [1_000, 5_000] {
        do_store_query_and_update(c, data_size)
    }
}

fn do_store_query_and_update(c: &mut Criterion, data_size: usize) {
    let data = read_bz2_data(&format!(
        "https://zenodo.org/records/12663333/files/dataset-{data_size}.nt.bz2"
    ));
    let explore_operations = bsbm_sparql_operation("exploreAndUpdate-1000.csv.bz2")
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
    let explore_query_operations = explore_operations
        .iter()
        .filter(|o| matches!(o, Operation::Query(_)))
        .cloned()
        .collect::<Vec<_>>();
    let business_operations = bsbm_sparql_operation("businessIntelligence-1000.csv.bz2")
        .into_iter()
        .filter_map(|op| match op {
            RawOperation::Query(q) => {
                // TODO remove once describe is supported
                if q.contains("DESCRIBE") {
                    None
                } else {
                    Some(Operation::Query(
                        Query::parse(&q.replace('#', ""), None).unwrap(),
                    ))
                }
            }
            RawOperation::Update(_) => unreachable!(),
        })
        .collect::<Vec<_>>();

    let mut group = c.benchmark_group("store operations");
    group.sample_size(10);

    {
        let runtime = Runtime::new().unwrap();
        let memory_store = Store::new();
        runtime.block_on(do_load(&memory_store, &data));

        group.bench_function(format!("BSBM explore {data_size} query in memory"), |b| {
            b.to_async(Runtime::new().unwrap())
                .iter(|| run_operation(&memory_store, &explore_query_operations));
        });
        group.bench_function(
            format!("BSBM explore {data_size} queryAndUpdate in memory"),
            |b| {
                b.to_async(Runtime::new().unwrap())
                    .iter(|| run_operation(&memory_store, &explore_operations));
            },
        );
        group.bench_function(
            format!("BSBM business intelligence {data_size} in memory"),
            |b| {
                b.to_async(Runtime::new().unwrap())
                    .iter(|| run_operation(&memory_store, &business_operations));
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

criterion_group!(store, store_query_and_update, store_load);

criterion_main!(store);

fn read_bz2_data(url: &str) -> Vec<u8> {
    let url = Url::from_str(url).unwrap();
    let file_name = url.path().split('/').next_back().unwrap().to_owned();

    if !Path::new(&file_name).exists() {
        let client = reqwest::blocking::Client::new();
        let response = client.get(url.clone()).send().unwrap();
        assert!(response.status().is_success(), "{}", &url);

        File::create(&file_name)
            .unwrap()
            .write_all(&response.bytes().unwrap())
            .unwrap();
    }

    let mut buf = Vec::new();
    MultiBzDecoder::new(File::open(&file_name).unwrap())
        .read_to_end(&mut buf)
        .unwrap();
    buf
}

fn bsbm_sparql_operation(file_name: &str) -> Vec<RawOperation> {
    csv::Reader::from_reader(read_bz2_data(&format!("https://zenodo.org/records/12663333/files/{file_name}")).as_slice()).records()
        .collect::<Result<Vec<_>, _>>().unwrap()
        .into_iter()
        .rev()
        .take(300) // We take only 10 groups
        .map(|l| {
            match &l[1] {
                "query" => RawOperation::Query(l[2].into()),
                "update" => RawOperation::Update(l[2].into()),
                _ => panic!("Unexpected operation kind {}", &l[1]),
            }
        })
        .collect()
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
