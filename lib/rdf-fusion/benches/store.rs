#![allow(clippy::panic)]

use codspeed_criterion_compat::{criterion_group, criterion_main, Criterion};
use futures::StreamExt;
use rdf_fusion::model::Term;
use rdf_fusion::store::Store;
use rdf_fusion_engine::results::QueryResults;
use rdf_fusion_model::{GraphName, NamedNode, Quad, Subject};
use tokio::runtime::Runtime;

/// This benchmark measures transactionally inserting synthetic quads into the store.
fn store_load(c: &mut Criterion) {
    c.bench_function("Store::load", |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| async {
            let store = Store::new();
            for quad in generate_quads(10_000) {
                store.insert(quad.as_ref()).await.unwrap();
            }
        });
    });
}

/// This benchmarks measure the duration of running a simple query (1 triple pattern) against an
/// empty store. Hopefully, this can provide insights into the "baseline" overhead of the query
/// engine.
fn store_empty_query(c: &mut Criterion) {
    c.bench_function("Store::query - Empty Store", |b| {
        b.to_async(Runtime::new().unwrap()).iter(|| async {
            let store = Store::new();
            let result = store.query("SELECT ?s ?p ?o {  ?s ?p ?o }").await.unwrap();
            match result {
                QueryResults::Solutions(mut sol) => {
                    assert!(matches!(sol.next().await, None), "No result expected");
                }
                _ => panic!("Unexpected QueryResults"),
            }
        });
    });
}

criterion_group!(store_write, store_load);
criterion_group!(store_query, store_empty_query);
criterion_main!(store_write, store_query);

fn generate_quads(count: u64) -> impl Iterator<Item = Quad> {
    (0..count).map(|i| {
        let subject = format!("http://example.com/subject{}", i);
        let predicate = format!("http://example.com/predicate{}", i);
        let object = format!("http://example.com/object{}", i);
        Quad::new(
            Subject::NamedNode(NamedNode::new_unchecked(subject)),
            NamedNode::new_unchecked(predicate),
            Term::NamedNode(NamedNode::new_unchecked(object)),
            GraphName::DefaultGraph,
        )
    })
}
