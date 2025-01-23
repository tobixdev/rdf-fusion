use crate::encoded::{EncTerm, EncTermField};
use crate::DFResult;
use datafusion::arrow::array::{ArrayRef, StringArray, StructArray};
use datafusion::arrow::datatypes::UnionMode;
use datafusion::common::{DataFusionError, ScalarValue};
use oxrdf::vocab::{rdf, xsd};
use oxrdf::{BlankNodeRef, GraphNameRef, LiteralRef, NamedNodeRef, SubjectRef, TermRef};
use std::str::FromStr;
use std::sync::Arc;

pub fn encode_scalar_graph(graph: GraphNameRef<'_>) -> ScalarValue {
    match graph {
        GraphNameRef::NamedNode(nn) => encode_scalar_named_node(nn),
        GraphNameRef::BlankNode(bnode) => encode_scalar_blank_node(bnode),
        GraphNameRef::DefaultGraph => ScalarValue::Union(
            Some((
                EncTermField::NamedNode.type_id(),
                Box::new(String::from("DEFAULT").into()),
            )),
            EncTerm::term_fields().clone(),
            UnionMode::Dense,
        ),
    }
}

pub fn encode_scalar_subject(subject: SubjectRef<'_>) -> ScalarValue {
    match subject {
        SubjectRef::NamedNode(nn) => encode_scalar_named_node(nn),
        SubjectRef::BlankNode(bnode) => encode_scalar_blank_node(bnode),
        _ => unimplemented!(),
    }
}

pub fn encode_scalar_predicate(predicate: NamedNodeRef<'_>) -> ScalarValue {
    encode_scalar_named_node(predicate)
}

pub fn encode_scalar_object(object: TermRef<'_>) -> DFResult<ScalarValue> {
    match object {
        TermRef::NamedNode(nn) => Ok(encode_scalar_named_node(nn)),
        TermRef::BlankNode(bnode) => Ok(encode_scalar_blank_node(bnode)),
        TermRef::Literal(lit) => encode_scalar_literal(lit),
        TermRef::Triple(_) => unimplemented!(),
    }
}

pub fn encode_scalar_literal(literal: LiteralRef<'_>) -> DFResult<ScalarValue> {
    if let Some(value) = try_specialize_literal(literal) {
        return value;
    }
    handle_generic_literal(literal)
}

pub fn encode_string(value: String) -> ScalarValue {
    let value = ScalarValue::Utf8(Some(value));
    ScalarValue::Union(
        Some((EncTermField::String.type_id(), Box::new(value))),
        EncTerm::term_fields(),
        UnionMode::Dense,
    )
}

pub fn encode_scalar_named_node(node: NamedNodeRef<'_>) -> ScalarValue {
    let value = ScalarValue::Utf8(Some(String::from(node.as_str())));
    ScalarValue::Union(
        Some((EncTermField::NamedNode.type_id(), Box::new(value))),
        EncTerm::term_fields(),
        UnionMode::Dense,
    )
}

pub fn encode_scalar_blank_node(node: BlankNodeRef<'_>) -> ScalarValue {
    let value = ScalarValue::Utf8(Some(String::from(&node.to_string()[2..])));
    ScalarValue::Union(
        Some((EncTermField::BlankNode.type_id(), Box::new(value))),
        EncTerm::term_fields(),
        UnionMode::Dense,
    )
}

fn try_specialize_literal(literal: LiteralRef<'_>) -> Option<DFResult<ScalarValue>> {
    let value = literal.value();
    let datatype = literal.datatype();
    match datatype {
        xsd::STRING | rdf::LANG_STRING => Some(specialize_string(literal, value)),
        xsd::BOOLEAN => Some(specialize_boolean(value)),
        xsd::FLOAT => Some(specialize_float32(value)),
        xsd::DOUBLE => Some(specialize_float64(value)),
        xsd::INTEGER => Some(specialize_integer(value)),
        xsd::INT => Some(specialize_int(value)),
        _ => None,
    }
}

fn specialize_string(literal: LiteralRef<'_>, value: &str) -> DFResult<ScalarValue> {
    let language = literal.language().map(String::from);
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![value])),
        Arc::new(StringArray::from(vec![language])),
    ];

    let structs = StructArray::new(EncTerm::string_fields(), arrays, None);
    let scalar_value = ScalarValue::Struct(Arc::new(structs));

    Ok(ScalarValue::Union(
        Some((EncTermField::String.type_id(), Box::new(scalar_value))),
        EncTerm::term_fields(),
        UnionMode::Dense,
    ))
}

fn specialize_boolean(value: &str) -> DFResult<ScalarValue> {
    specialize_primitive(value, EncTermField::Boolean, |value: bool| {
        ScalarValue::Boolean(Some(value))
    })
}

fn specialize_float32(value: &str) -> DFResult<ScalarValue> {
    specialize_primitive(value, EncTermField::Float32, |value: f32| {
        ScalarValue::Float32(Some(value))
    })
}

fn specialize_float64(value: &str) -> DFResult<ScalarValue> {
    specialize_primitive(value, EncTermField::Float64, |value: f64| {
        ScalarValue::Float64(Some(value))
    })
}

fn specialize_int(value: &str) -> DFResult<ScalarValue> {
    specialize_primitive(value, EncTermField::Int, |value: i32| {
        ScalarValue::Int32(Some(value))
    })
}

fn specialize_integer(value: &str) -> DFResult<ScalarValue> {
    specialize_primitive(value, EncTermField::Integer, |value: i64| {
        ScalarValue::Int64(Some(value))
    })
}

fn specialize_primitive<T, F>(
    value: &str,
    term_field: EncTermField,
    map: F,
) -> DFResult<ScalarValue>
where
    T: FromStr,
    F: Fn(T) -> ScalarValue,
    <T as FromStr>::Err: std::fmt::Display,
{
    let value = value.parse::<T>().map(map).map_err(|inner| {
        DataFusionError::Execution(format!("Could not parse primitive: {}", inner))
    })?;
    Ok(ScalarValue::Union(
        Some((term_field.type_id(), Box::new(value))),
        EncTerm::term_fields(),
        UnionMode::Dense,
    ))
}

fn handle_generic_literal(literal: LiteralRef<'_>) -> DFResult<ScalarValue> {
    let value = literal.value();
    let datatype = literal.datatype().as_str();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![value])),
        Arc::new(StringArray::from(vec![datatype.to_string()])),
    ];
    let structs = StructArray::new(EncTerm::typed_literal_fields(), arrays, None);
    let scalar_value = ScalarValue::Struct(Arc::new(structs));
    Ok(ScalarValue::Union(
        Some((EncTermField::TypedLiteral.type_id(), Box::new(scalar_value))),
        EncTerm::term_fields().clone(),
        UnionMode::Dense,
    ))
}
