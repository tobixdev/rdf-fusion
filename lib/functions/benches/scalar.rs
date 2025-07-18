use codspeed_criterion_compat::{criterion_group, criterion_main, Criterion};
use datafusion::arrow::datatypes::Field;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use rdf_fusion_encoding::typed_value::{TypedValueArrayBuilder, TypedValueEncoding};
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_functions::builtin::BuiltinName;
use rdf_fusion_functions::registry::{DefaultRdfFusionFunctionRegistry, RdfFusionFunctionRegistry};
use rdf_fusion_functions::{FunctionName, RdfFusionFunctionArgs};
use rdf_fusion_model::NamedNodeRef;
use std::sync::Arc;

fn is_iri(c: &mut Criterion) {
    let registry = DefaultRdfFusionFunctionRegistry::default();

    let is_iri = registry
        .create_udf(
            FunctionName::Builtin(BuiltinName::IsIri),
            RdfFusionFunctionArgs::empty(),
        )
        .unwrap();

    let input_field = Arc::new(Field::new("input", TypedValueEncoding::data_type(), true));
    let return_field = Arc::new(Field::new("result", TypedValueEncoding::data_type(), true));

    let mut payload_builder = TypedValueArrayBuilder::default();
    for i in 0..8192 {
        payload_builder
            .append_named_node(NamedNodeRef::new_unchecked(
                format!("http://example.com/{}", i).as_str(),
            ))
            .unwrap();
    }
    let payload = payload_builder.finish();

    c.bench_function("IS_IRI", |b| {
        b.iter(|| {
            let args = ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(payload.clone())],
                arg_fields: vec![input_field.clone()],
                number_rows: 8192,
                return_field: return_field.clone(),
            };
            is_iri.invoke_with_args(args).unwrap();
        });
    });
}

criterion_group!(terms, is_iri);
criterion_main!(terms);
