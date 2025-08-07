use rdf_fusion_api::storage::QuadStorage;
use rdf_fusion_model::{
    GraphName, GraphNameRef, Literal, NamedNode, NamedOrBlankNode, Quad, Subject, Term,
};
use rdf_fusion_storage::MemoryQuadStorage;
use tokio;

#[tokio::test]
async fn test_insert_quad() {
    let store = MemoryQuadStorage::new();

    let inserted = store.insert_quads(vec![example_quad()]).await.unwrap();
    assert_eq!(inserted, 1);

    let len = store.len().await.unwrap();
    assert_eq!(len, 1);
}

#[tokio::test]
async fn test_insert_duplicate_quads_no_effect() {
    let store = MemoryQuadStorage::new();

    store.insert_quads(vec![example_quad()]).await.unwrap();

    let inserted = store.insert_quads(vec![example_quad()]).await.unwrap();
    assert_eq!(inserted, 0); // duplicate
}

#[tokio::test]
async fn test_insert_duplicate_quads_in_same_operation_quads() {
    let store = MemoryQuadStorage::new();

    let inserted = store
        .insert_quads(vec![example_quad(), example_quad()])
        .await
        .unwrap();

    assert_eq!(inserted, 1);
}

#[tokio::test]
async fn test_named_graph_insertion_and_query() {
    let store = MemoryQuadStorage::new();
    let graph =
        NamedOrBlankNode::NamedNode(NamedNode::new("http://example.com/graph").unwrap());

    let inserted = store.insert_named_graph(graph.as_ref()).await.unwrap();
    assert!(inserted);

    let exists = store.contains_named_graph(graph.as_ref()).await.unwrap();
    assert!(exists);

    let graphs = store.named_graphs().await.unwrap();
    assert_eq!(graphs.len(), 1);
    assert_eq!(graphs[0], graph);
}

#[tokio::test]
async fn test_remove_quad() {
    let store = MemoryQuadStorage::new();
    let quad = example_quad_in_graph("http://example.com/g");

    store.insert_quads(vec![quad.clone()]).await.unwrap();
    let removed = store.remove(quad.as_ref()).await.unwrap();
    assert!(removed);

    let len = store.len().await.unwrap();
    assert_eq!(len, 0);
}

#[tokio::test]
async fn test_clear_graph() {
    let store = MemoryQuadStorage::new();

    let g1 = "http://example.com/g1";
    let g2 = "http://example.com/g2";

    store
        .insert_quads(vec![example_quad_in_graph(g1), example_quad_in_graph(g2)])
        .await
        .unwrap();

    store
        .clear_graph(GraphNameRef::NamedNode(
            NamedNode::new(g1).unwrap().as_ref(),
        ))
        .await
        .unwrap();

    let len = store.len().await.unwrap();
    assert_eq!(len, 1);
}

#[tokio::test]
async fn test_remove_named_graph() {
    let store = MemoryQuadStorage::new();
    let graph =
        NamedOrBlankNode::NamedNode(NamedNode::new("http://example.com/graph").unwrap());

    store.insert_named_graph(graph.as_ref()).await.unwrap();
    let removed = store.remove_named_graph(graph.as_ref()).await.unwrap();
    assert!(removed);

    let exists = store.contains_named_graph(graph.as_ref()).await.unwrap();
    assert!(!exists);
}

#[tokio::test]
async fn test_clear_all() {
    let store = MemoryQuadStorage::new();
    store
        .insert_quads(vec![
            example_quad_in_graph("http://example.com/g1"),
            example_quad_in_graph("http://example.com/g2"),
        ])
        .await
        .unwrap();

    store.clear().await.unwrap();
    let len = store.len().await.unwrap();
    assert_eq!(len, 0);
}

#[tokio::test]
async fn test_snapshot_consistency() {
    let store = MemoryQuadStorage::new();
    store
        .insert_quads(vec![example_quad_in_graph("http://g")])
        .await
        .unwrap();

    let snapshot = store.snapshot();

    // Update storage after snapshot
    store.clear().await.unwrap();

    // Snapshot should still see the original quad
    assert_eq!(snapshot.len(), 1);
}

#[tokio::test]
async fn test_validate_storage() {
    let store = MemoryQuadStorage::new();
    store
        .insert_quads(vec![example_quad_in_graph("http://g")])
        .await
        .unwrap();

    let result = store.validate().await;
    assert!(result.is_ok());
}

fn example_quad() -> Quad {
    Quad::new(
        Subject::NamedNode(NamedNode::new("http://example.com/subject").unwrap()),
        NamedNode::new("http://example.com/predicate").unwrap(),
        Term::Literal(Literal::new_simple_literal("value")),
        GraphName::DefaultGraph,
    )
}

fn example_quad_in_graph(graph: &str) -> Quad {
    Quad::new(
        Subject::NamedNode(NamedNode::new("http://example.com/subject").unwrap()),
        NamedNode::new("http://example.com/predicate").unwrap(),
        Term::Literal(Literal::new_simple_literal("value")),
        GraphName::NamedNode(NamedNode::new(graph).unwrap()),
    )
}
