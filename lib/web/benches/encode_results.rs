use axum_test::TestServer;
use codspeed_criterion_compat::{Criterion, criterion_group, criterion_main};
use rdf_fusion::model::{GraphName, NamedNode, Quad, Subject, Term};
use rdf_fusion::store::Store;
use rdf_fusion_web::{AppState, create_router};
use tokio::runtime::Builder;

fn encode_solution(criterion: &mut Criterion) {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();

    let store = Store::new();
    runtime.block_on(async {
        for quad in generate_quads(8192) {
            store.insert(quad.as_ref()).await.unwrap();
        }
    });

    let app_state = AppState {
        store,
        read_only: false,
        union_default_graph: false,
    };

    let app = create_router(app_state);
    let server = TestServer::new(app).unwrap();

    criterion.bench_function("Web: Encode SELECT Result", |b| {
        b.to_async(&runtime).iter(async || {
            let query = "SELECT * WHERE { ?s ?p ?o }";
            let response = server
                .get("/repositories/default/query")
                .add_query_param("query", query)
                .expect_success()
                .await;
            assert_eq!(response.text().len(), 1495862);
        })
    });
}

fn generate_quads(count: usize) -> impl Iterator<Item = Quad> {
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

criterion_group!(encode_results, encode_solution);
criterion_main!(encode_results);
