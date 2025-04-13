use crate::encoded::write_enc_term::WriteEncTerm;
use crate::encoded::{EncTerm, EncTermField};
use crate::DFResult;
use datafusion::arrow::array::{ArrayRef, StringArray, StructArray};
use datafusion::arrow::datatypes::UnionMode;
use datafusion::common::{DataFusionError, ScalarValue};
use datamodel::{BlankNodeRef, Boolean, Date, DateTime, DayTimeDuration, Decimal, DecodedTermRef, Double, Duration, Float, GraphNameRef, Int, Integer, LiteralRef, NamedNodeRef, SubjectRef, Time, YearMonthDuration};
use oxrdf::vocab::{rdf, xsd};
use std::str::FromStr;
use std::sync::Arc;

pub fn encode_scalar_graph(graph: GraphNameRef<'_>) -> ScalarValue {
    match graph {
        GraphNameRef::NamedNode(nn) => encode_scalar_named_node(nn),
        GraphNameRef::BlankNode(bnode) => encode_scalar_blank_node(bnode),
        GraphNameRef::DefaultGraph => encode_scalar_null(),
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

pub fn encode_scalar_term(object: DecodedTermRef<'_>) -> DFResult<ScalarValue> {
    match object {
        DecodedTermRef::NamedNode(nn) => Ok(encode_scalar_named_node(nn)),
        DecodedTermRef::BlankNode(bnode) => Ok(encode_scalar_blank_node(bnode)),
        DecodedTermRef::Literal(lit) => encode_scalar_literal(lit),
        DecodedTermRef::Triple(_) => unimplemented!(),
    }
}

pub fn encode_scalar_literal(literal: LiteralRef<'_>) -> DFResult<ScalarValue> {
    if let Some(value) = try_specialize_literal(literal) {
        return value;
    }
    handle_generic_literal(literal)
}

pub fn encode_string(value: String) -> ScalarValue {
    // TODO: shouldn't this be a struct?
    let value = ScalarValue::Utf8(Some(value));
    ScalarValue::Union(
        Some((EncTermField::String.type_id(), Box::new(value))),
        EncTerm::term_fields(),
        UnionMode::Dense,
    )
}

pub fn encode_scalar_null() -> ScalarValue {
    ScalarValue::Union(
        Some((EncTermField::Null.type_id(), Box::new(ScalarValue::Null))),
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
    // TODO: Move these constants to the datamodel
    match datatype {
        xsd::STRING | rdf::LANG_STRING => Some(specialize_string(literal, value)),
        xsd::BOOLEAN => Some(specialize_parsable::<Boolean>(value)),
        xsd::FLOAT => Some(specialize_parsable::<Float>(value)),
        xsd::DECIMAL => Some(specialize_parsable::<Decimal>(value)),
        xsd::DOUBLE => Some(specialize_parsable::<Double>(value)),
        xsd::INTEGER => Some(specialize_parsable::<Integer>(value)),
        xsd::INT => Some(specialize_parsable::<Int>(value)),
        xsd::DATE_TIME => Some(specialize_parsable::<DateTime>(value)),
        xsd::TIME => Some(specialize_parsable::<Time>(value)),
        xsd::DATE => Some(specialize_parsable::<Date>(value)),
        xsd::DURATION => Some(specialize_parsable::<Duration>(value)),
        xsd::YEAR_MONTH_DURATION => Some(specialize_parsable::<YearMonthDuration>(value)),
        xsd::DAY_TIME_DURATION => Some(specialize_parsable::<DayTimeDuration>(value)),
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

fn specialize_parsable<T>(value: &str) -> DFResult<ScalarValue>
where
    T: FromStr,
    T: WriteEncTerm,
    <T as FromStr>::Err: std::fmt::Display,
{
    let value = value.parse::<T>().map_err(|inner| {
        DataFusionError::Execution(format!("Could not parse primitive: {}", inner))
    })?;
    Ok(value.into_scalar_value()?)
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
