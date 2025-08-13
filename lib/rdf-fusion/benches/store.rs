#![allow(clippy::panic)]

use codspeed_criterion_compat::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use rdf_fusion::model::Term;
use rdf_fusion::store::Store;
use rdf_fusion_execution::results::QueryResults;
use rdf_fusion_model::{GraphName, NamedNode, Quad, Subject};
use tokio::runtime::Builder;

/// This benchmark measures transactionally inserting synthetic quads into the store.
fn store_load(c: &mut Criterion) {
    c.bench_function("Store::load", |b| {
        b.to_async(&Builder::new_current_thread().enable_all().build().unwrap())
            .iter(|| async {
                let store = Store::new();
                for quad in generate_quads(10_000) {
                    store.insert(quad.as_ref()).await.unwrap();
                }
            });
    });
}

/// These benchmarks measure the duration of running a simple query (1 triple pattern). Hopefully,
/// this can provide insights into the "baseline" overhead of the query engine.
fn store_single_pattern(c: &mut Criterion) {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();

    // No Quads
    c.bench_function("Store::query - Single Pattern / No Quads", |b| {
        let store = runtime.block_on(prepare_store_with_generated_triples(0));
        b.to_async(&runtime).iter(|| trivial_query(&store, 0));
    });
    // One Quad
    c.bench_function("Store::query - Single Pattern / Single Quad", |b| {
        let store = runtime.block_on(prepare_store_with_generated_triples(1));
        b.to_async(&runtime).iter(|| trivial_query(&store, 1));
    });
    // One Record Batch
    c.bench_function("Store::query - Single Pattern / 8192 Quads", |b| {
        let store = runtime.block_on(prepare_store_with_generated_triples(8192));
        b.to_async(&runtime).iter(|| trivial_query(&store, 8192));
    });
}

/// These benchmarks measure the duration of running a simple query that fixes a single part of the
/// pattern (i.e., subject, predicate, object, graph).
fn store_single_pattern_with_fixed_element(c: &mut Criterion) {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();

    // Subject
    c.bench_function(
        "Store::query - Single Pattern With Fixed Element (subject)",
        |b| {
            let store = runtime.block_on(prepare_store_with_generated_triples(8192));
            b.to_async(&runtime).iter(|| async {
                let result = store
                    .query("SELECT ?p ?o { <http://example.com/subject0> ?p ?o }")
                    .await
                    .unwrap();
                assert_number_of_results(result, 1).await;
            });
        },
    );

    // Predicate
    c.bench_function(
        "Store::query - Single Pattern With Fixed Element (predicate)",
        |b| {
            let store = runtime.block_on(prepare_store_with_generated_triples(8192));
            b.to_async(&runtime).iter(|| async {
                let result = store
                    .query("SELECT ?s ?o { ?s <http://example.com/predicate0> ?o }")
                    .await
                    .unwrap();
                assert_number_of_results(result, 1).await;
            });
        },
    );

    // Object
    c.bench_function(
        "Store::query - Single Pattern With Fixed Element (object)",
        |b| {
            let store = runtime.block_on(prepare_store_with_generated_triples(8192));
            b.to_async(&runtime).iter(|| async {
                let result = store
                    .query("SELECT ?s ?p { ?s ?p <http://example.com/object0> }")
                    .await
                    .unwrap();
                assert_number_of_results(result, 1).await;
            });
        },
    );

    c.bench_function("Store::query - Scan Objects", |b| {
        let store = runtime.block_on(async {
            let store = Store::new();
            let quads = (0..500_000).map(|i| {
                let subject = "http://example.com/subject";
                let predicate = "http://example.com/predicate";
                let object = format!("http://example.com/object{i}");
                Quad::new(
                    Subject::NamedNode(NamedNode::new_unchecked(subject)),
                    NamedNode::new_unchecked(predicate),
                    Term::NamedNode(NamedNode::new_unchecked(object)),
                    GraphName::DefaultGraph,
                )
            });
            for quad in quads {
                store.insert(quad.as_ref()).await.unwrap();
            }
            store
        });
        b.to_async(&runtime).iter(|| async {
            let result = store
                .query("SELECT ?o { <http://example.com/subject> <http://example.com/predicate> ?o }")
                .await
                .unwrap();
            assert_number_of_results(result, 500_000).await;
        });
    });
}

criterion_group!(store_write, store_load);
criterion_group!(
    store_query,
    store_single_pattern,
    store_single_pattern_with_fixed_element
);
criterion_main!(store_write, store_query);

async fn prepare_store_with_generated_triples(n: usize) -> Store {
    let store = Store::new();
    for quad in generate_quads(n) {
        store.insert(quad.as_ref()).await.unwrap();
    }
    store
}

fn generate_quads(count: usize) -> impl Iterator<Item = Quad> {
    (0..count).map(|i| {
        let subject = format!("http://example.com/subject{i}");
        let predicate = format!("http://example.com/predicate{i}");
        let object = format!("http://example.com/object{i}");
        Quad::new(
            Subject::NamedNode(NamedNode::new_unchecked(subject)),
            NamedNode::new_unchecked(predicate),
            Term::NamedNode(NamedNode::new_unchecked(object)),
            GraphName::DefaultGraph,
        )
    })
}

async fn trivial_query(store: &Store, n: usize) {
    let result = store.query("SELECT ?s ?p ?o { ?s ?p ?o }").await.unwrap();
    assert_number_of_results(result, n).await;
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
        _ => panic!("Unexpected QueryResults"),
    }
}
