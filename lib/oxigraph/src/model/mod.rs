//! Implements data structures for [RDF 1.1 Concepts](https://www.w3.org/TR/rdf11-concepts/) using [OxRDF](https://crates.io/crates/oxrdf).
//!
//! Usage example:
//!
//! ```
//! use oxigraph::model::*;
//!
//! let mut graph = Graph::default();
//!
//! // insertion
//! let ex = NamedNodeRef::new("http://example.com").unwrap();
//! let triple = TripleRef::new(ex, ex, ex);
//! graph.insert(triple);
//!
//! // simple filter
//! let results: Vec<_> = graph.triples_for_subject(ex).collect();
//! assert_eq!(vec![triple], results);
//! ```

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, UnionFields, UnionMode};
pub use oxrdf::*;

pub(crate) fn single_quad_table_schema() -> Schema {
    let iri_type = DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8));
    let bnode_type = DataType::UInt32;
    let literal_type = DataType::Struct(Fields::from(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("type", iri_type.clone(), false),
        // TODO: Language Tags
    ]));

    let subject_type = DataType::Union(
        UnionFields::new(
            vec![0, 1],
            vec![
                Field::new("iri", iri_type.clone(), true),
                Field::new("bnode", bnode_type.clone(), true),
            ],
        ),
        UnionMode::Sparse,
    );
    let predicate_type = iri_type.clone();
    let object_type = DataType::Union(
        UnionFields::new(
            vec![0, 1, 2],
            vec![
                Field::new("iri", iri_type.clone(), true),
                Field::new("bnode", bnode_type.clone(), true),
                Field::new("literal", literal_type.clone(), true),
                // TODO: Add triple
            ],
        ),
        UnionMode::Sparse,
    );

    Schema::new(vec![
        Field::new("graph_name", iri_type.clone(), false),
        Field::new("subject", subject_type, false),
        Field::new("predicate", predicate_type, false),
        Field::new("object", object_type, false),
    ])
}