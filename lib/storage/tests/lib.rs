use futures::StreamExt;
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_model::{GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
use tokio;

mod memory;

fn example_quad() -> Quad {
    Quad::new(
        NamedOrBlankNode::NamedNode(
            NamedNode::new("http://example.com/subject").unwrap(),
        ),
        NamedNode::new("http://example.com/predicate").unwrap(),
        Term::Literal(Literal::new_simple_literal("value")),
        GraphName::DefaultGraph,
    )
}

fn example_quad_in_graph(graph: &str) -> Quad {
    Quad::new(
        NamedOrBlankNode::NamedNode(
            NamedNode::new("http://example.com/subject").unwrap(),
        ),
        NamedNode::new("http://example.com/predicate").unwrap(),
        Term::Literal(Literal::new_simple_literal("value")),
        GraphName::NamedNode(NamedNode::new(graph).unwrap()),
    )
}
