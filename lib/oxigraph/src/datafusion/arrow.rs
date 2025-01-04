use datafusion::arrow::array::{ArrayBuilder, RecordBatch, StringBuilder, UnionArray};
use datafusion::arrow::datatypes::{
    DataType, Field, Fields, Schema, SchemaRef, UnionFields, UnionMode,
};
use datafusion::arrow::error::ArrowError;
use once_cell::sync::Lazy;
use oxrdf::{Quad, Subject, Term};
use std::sync::Arc;

pub(crate) static IRI_TYPE: Lazy<DataType> = Lazy::new(|| DataType::Utf8);
pub(crate) static BNODE_TYPE: Lazy<DataType> = Lazy::new(|| DataType::Utf8);
pub(crate) static LITERAL_TYPE: Lazy<DataType> = Lazy::new(|| {
    DataType::Struct(Fields::from(vec![
        Field::new("value", DataType::Utf8, false),
        // TODO: Type and Language Tags
    ]))
});
pub(crate) static SUBJECT_UNION_FIELDS: Lazy<UnionFields> = Lazy::new(|| {
    UnionFields::new(
        vec![0, 1],
        vec![
            Field::new("iri", IRI_TYPE.clone(), true),
            Field::new("bnode", BNODE_TYPE.clone(), true),
        ],
    )
});
pub(crate) static SUBJECT_TYPE: Lazy<DataType> =
    Lazy::new(|| DataType::Union(SUBJECT_UNION_FIELDS.clone(), UnionMode::Sparse));
pub(crate) static PREDICATE_TYPE: Lazy<DataType> = Lazy::new(|| IRI_TYPE.clone());
pub(crate) static OBJECT_UNION_FIELDS: Lazy<UnionFields> = Lazy::new(|| {
    UnionFields::new(
        vec![0, 1, 2],
        vec![
            Field::new("iri", IRI_TYPE.clone(), true),
            Field::new("bnode", BNODE_TYPE.clone(), true),
            Field::new("literal", LITERAL_TYPE.clone(), true),
            // TODO: Add triple
        ],
    )
});
pub(crate) static OBJECT_TYPE: Lazy<DataType> =
    Lazy::new(|| DataType::Union(OBJECT_UNION_FIELDS.clone(), UnionMode::Sparse));
pub(crate) static SINGLE_QUAD_TABLE_SCHEMA: Lazy<Schema> = Lazy::new(|| {
    Schema::new(vec![
        Field::new("graph", IRI_TYPE.clone(), false),
        Field::new("subject", SUBJECT_TYPE.clone(), false),
        Field::new("predicate", PREDICATE_TYPE.clone(), false),
        Field::new("object", OBJECT_TYPE.clone(), false),
    ])
});

pub(crate) fn single_quad_table_schema_encode_quads(
    quads: Vec<Quad>,
) -> Result<Vec<RecordBatch>, ArrowError> {
    // Create builders
    let mut graph_builder = StringBuilder::new();
    let mut subject_type_ids = Vec::new();
    let mut subject_iri_builder = StringBuilder::new();
    let mut subject_bnode_builder = StringBuilder::new();
    let mut predicate_builder = StringBuilder::new();
    let mut object_type_ids = Vec::new();
    let mut object_iri_builder = StringBuilder::new();
    let mut object_bnode_builder = StringBuilder::new();
    let mut object_literal_value_builder = StringBuilder::new();

    // Populate builders with data
    for quad in quads {
        graph_builder.append_value(&quad.graph_name.to_string());

        // Subject
        match &quad.subject {
            Subject::NamedNode(nn) => {
                subject_type_ids.push(0);
                subject_iri_builder.append_value(nn.as_str());
                subject_bnode_builder.append_null();
            }
            Subject::BlankNode(bnode) => {
                subject_type_ids.push(1);
                subject_iri_builder.append_null();
                subject_bnode_builder.append_value(bnode.as_str());
            }
            Subject::Triple(_) => unimplemented!(),
        };

        predicate_builder.append_value(&quad.predicate.to_string());

        // Object
        match &quad.object {
            Term::NamedNode(nn) => {
                object_type_ids.push(0);
                object_iri_builder.append_value(nn.as_str());
                object_bnode_builder.append_null();
                object_literal_value_builder.append_null();
            }
            Term::BlankNode(bnode) => {
                object_type_ids.push(1);
                object_iri_builder.append_null();
                object_bnode_builder.append_value(bnode.as_str());
                object_literal_value_builder.append_null();
            }
            Term::Literal(literal) => {
                object_type_ids.push(2);
                object_iri_builder.append_null();
                object_bnode_builder.append_null();
                object_literal_value_builder.append_value(literal.value());
            }
            Term::Triple(_) => unimplemented!(),
        };
    }

    // Build arrays
    let subjects = UnionArray::try_new(
        SUBJECT_UNION_FIELDS.clone(),
        subject_type_ids.into_iter().collect(),
        None,
        vec![
            Arc::new(subject_iri_builder.finish()),
            Arc::new(subject_bnode_builder.finish()),
        ],
    )?;

    let objects = UnionArray::try_new(
        OBJECT_UNION_FIELDS.clone(),
        object_type_ids.into_iter().collect(),
        None,
        vec![
            Arc::new(object_iri_builder.finish()),
            Arc::new(object_bnode_builder.finish()),
            Arc::new(object_literal_value_builder.finish()),
        ],
    )?;

    // Create record batch
    Ok(vec![RecordBatch::try_new(
        SchemaRef::new(SINGLE_QUAD_TABLE_SCHEMA.clone()),
        vec![
            Arc::new(graph_builder.finish()),
            Arc::new(subjects),
            Arc::new(predicate_builder.finish()),
            Arc::new(objects),
        ],
    )?])
}
