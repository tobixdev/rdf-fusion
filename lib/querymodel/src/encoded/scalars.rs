use crate::encoded::{FIELDS_TERM, TYPE_ID_STRING};
use datafusion::arrow::datatypes::UnionMode;
use datafusion::common::ScalarValue;
use oxrdf::{GraphNameRef, LiteralRef, NamedNodeRef, SubjectRef, TermRef};

pub fn scalar_graph(graph: &GraphNameRef<'_>) -> ScalarValue {
    let graph = graph.to_string();
    encode_string(graph)
}

pub fn scalar_subject(subject: &SubjectRef<'_>) -> ScalarValue {
    match subject {
        SubjectRef::NamedNode(nn) => encode_string(nn.to_string()),
        SubjectRef::BlankNode(bnode) => encode_string(bnode.to_string()),
        SubjectRef::Triple(_) => unimplemented!(),
    }
}

pub fn scalar_predicate(predicate: &NamedNodeRef<'_>) -> ScalarValue {
    encode_string(predicate.to_string())
}

pub fn scalar_object(object: &TermRef<'_>) -> ScalarValue {
    match object {
        TermRef::NamedNode(nn) => encode_string(nn.to_string()),
        TermRef::BlankNode(bnode) => encode_string(bnode.to_string()),
        TermRef::Literal(lit) => scalar_literal(lit),
        TermRef::Triple(_) => unimplemented!(),
    }
}

pub fn scalar_literal(literal: &LiteralRef<'_>) -> ScalarValue {
    encode_string(literal.to_string())
}

pub fn encode_string(value: String) -> ScalarValue {
    let value = ScalarValue::Utf8View(Some(value));
    ScalarValue::Union(
        Some((*TYPE_ID_STRING, Box::new(value))),
        FIELDS_TERM.clone(),
        UnionMode::Dense,
    )
}
