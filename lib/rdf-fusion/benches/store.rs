#![allow(clippy::panic)]

use codspeed_criterion_compat::{criterion_group, criterion_main, Criterion};
use rdf_fusion::model::Term;
use rdf_fusion::store::Store;
use rdf_fusion_model::{GraphName, NamedNode, Quad, Subject};
use tokio::runtime::Runtime;

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

criterion_group!(store, store_load);
criterion_main!(store);

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
