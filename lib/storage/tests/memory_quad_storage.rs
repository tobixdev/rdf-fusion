use rdf_fusion_api::storage::QuadStorage;
use rdf_fusion_model::{
    GraphName, GraphNameRef, Literal, NamedNode, NamedOrBlankNode, Quad, Subject, Term,
};
use rdf_fusion_storage::memory::{MemObjectIdMapping, MemQuadStorage};
use std::sync::Arc;
use tokio;

#[tokio::test]
async fn insert_quad() {
    let storage = create_storage();

    let inserted = storage.insert_quads(vec![example_quad()]).await.unwrap();
    assert_eq!(inserted, 1);

    let len = storage.len().await.unwrap();
    assert_eq!(len, 1);
}

#[tokio::test]
async fn insert_quad_returns_correct_quad() {
    let storage = create_storage();

    storage.insert_quads(vec![example_quad()]).await.unwrap();

    todo!()
}

#[tokio::test]
async fn insert_duplicate_quads_no_effect() {
    let storage = create_storage();

    storage.insert_quads(vec![example_quad()]).await.unwrap();

    let inserted = storage.insert_quads(vec![example_quad()]).await.unwrap();
    assert_eq!(inserted, 0); // duplicate
}

#[tokio::test]
async fn insert_duplicate_quads_in_same_operation_quads() {
    let storage = create_storage();

    let inserted = storage
        .insert_quads(vec![example_quad(), example_quad()])
        .await
        .unwrap();

    assert_eq!(inserted, 1);
}

#[tokio::test]
async fn named_graph_insertion_and_query() {
    let storage = create_storage();
    let graph =
        NamedOrBlankNode::NamedNode(NamedNode::new("http://example.com/graph").unwrap());

    let inserted = storage.insert_named_graph(graph.as_ref()).await.unwrap();
    assert!(inserted);

    let exists = storage.contains_named_graph(graph.as_ref()).await.unwrap();
    assert!(exists);

    let graphs = storage.named_graphs().await.unwrap();
    assert_eq!(graphs.len(), 1);
    assert_eq!(graphs[0], graph);
}

#[tokio::test]
async fn remove_quad() {
    let storage = create_storage();
    let quad = example_quad_in_graph("http://example.com/g");

    storage.insert_quads(vec![quad.clone()]).await.unwrap();
    let removed = storage.remove(quad.as_ref()).await.unwrap();
    assert!(removed);

    let len = storage.len().await.unwrap();
    assert_eq!(len, 0);
}

#[tokio::test]
async fn clear_graph() {
    let storage = create_storage();

    let g1 = "http://example.com/g1";
    let g2 = "http://example.com/g2";

    storage
        .insert_quads(vec![example_quad_in_graph(g1), example_quad_in_graph(g2)])
        .await
        .unwrap();

    storage
        .clear_graph(GraphNameRef::NamedNode(
            NamedNode::new(g1).unwrap().as_ref(),
        ))
        .await
        .unwrap();

    let len = storage.len().await.unwrap();
    assert_eq!(len, 1);
}

#[tokio::test]
async fn insert_named_graph() {
    let storage = create_storage();
    let graph =
        NamedOrBlankNode::NamedNode(NamedNode::new("http://example.com/graph").unwrap());
    storage.insert_named_graph(graph.as_ref()).await.unwrap();
    let exists = storage.contains_named_graph(graph.as_ref()).await.unwrap();
    assert!(exists);
}

#[tokio::test]
async fn remove_named_graph() {
    let storage = create_storage();
    let graph =
        NamedOrBlankNode::NamedNode(NamedNode::new("http://example.com/graph").unwrap());

    storage.insert_named_graph(graph.as_ref()).await.unwrap();
    let removed = storage.remove_named_graph(graph.as_ref()).await.unwrap();
    assert!(removed);

    let exists = storage.contains_named_graph(graph.as_ref()).await.unwrap();
    assert!(!exists);
}

#[tokio::test]
async fn clear_all() {
    let storage = create_storage();
    storage
        .insert_quads(vec![
            example_quad_in_graph("http://example.com/g1"),
            example_quad_in_graph("http://example.com/g2"),
        ])
        .await
        .unwrap();

    storage.clear().await.unwrap();
    let len = storage.len().await.unwrap();
    assert_eq!(len, 0);
}

#[tokio::test]
async fn snapshot_consistency() {
    let storage = create_storage();
    storage
        .insert_quads(vec![example_quad_in_graph("http://g")])
        .await
        .unwrap();

    let snapshot = storage.snapshot();

    // Update storage after snapshot
    storage.clear().await.unwrap();

    // Snapshot should still see the original quad
    assert_eq!(snapshot.len().await, 1);
}

#[tokio::test]
async fn validate_storage() {
    let storage = create_storage();

    storage
        .insert_quads(vec![example_quad_in_graph("http://g")])
        .await
        .unwrap();

    let result = storage.validate().await;
    assert!(result.is_ok());
}

fn create_storage() -> MemQuadStorage {
    let object_id_encoding = MemObjectIdMapping::new();
    MemQuadStorage::new(Arc::new(object_id_encoding))
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
