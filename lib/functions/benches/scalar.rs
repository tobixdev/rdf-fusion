use codspeed_criterion_compat::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::datatypes::Field;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
use rdf_fusion_encoding::typed_value::{
    TYPED_VALUE_ENCODING, TypedValueArrayElementBuilder,
};
use rdf_fusion_encoding::{EncodingArray, RdfFusionEncodings, TermEncoding};
use rdf_fusion_extensions::functions::{
    BuiltinName, FunctionName, RdfFusionFunctionRegistry,
};
use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
use rdf_fusion_model::{BlankNode, Float, Integer, NamedNodeRef};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
enum UnaryScenario {
    AllNamedNodes,
    Mixed,
    AllBlank,
    AllInt,
    AllFloat,
    AllString,
}

impl UnaryScenario {
    fn create_args(&self) -> Vec<ColumnarValue> {
        match self {
            UnaryScenario::AllNamedNodes => {
                let mut payload_builder = TypedValueArrayElementBuilder::default();
                for i in 0..8192 {
                    payload_builder
                        .append_named_node(NamedNodeRef::new_unchecked(
                            format!("http://example.com/{i}").as_str(),
                        ))
                        .unwrap();
                }
                vec![ColumnarValue::Array(payload_builder.finish().into_array())]
            }
            UnaryScenario::Mixed => {
                let mut payload_builder = TypedValueArrayElementBuilder::default();
                for i in 0..8192 {
                    match i % 4 {
                        0 => {
                            payload_builder
                                .append_named_node(NamedNodeRef::new_unchecked(
                                    format!("http://example.com/{i}").as_str(),
                                ))
                                .unwrap();
                        }
                        1 => {
                            payload_builder.append_integer(Integer::from(i)).unwrap();
                        }
                        2 => {
                            payload_builder.append_float(Float::from(i as i16)).unwrap();
                        }
                        _ => {
                            payload_builder
                                .append_blank_node(BlankNode::default().as_ref())
                                .unwrap();
                        }
                    }
                }
                vec![ColumnarValue::Array(payload_builder.finish().into_array())]
            }
            UnaryScenario::AllBlank => {
                let mut payload_builder = TypedValueArrayElementBuilder::default();
                for _ in 0..8192 {
                    payload_builder
                        .append_blank_node(BlankNode::default().as_ref())
                        .unwrap();
                }
                vec![ColumnarValue::Array(payload_builder.finish().into_array())]
            }
            UnaryScenario::AllInt => {
                let mut payload_builder = TypedValueArrayElementBuilder::default();
                for i in 0..8192 {
                    payload_builder.append_integer(Integer::from(i)).unwrap();
                }
                vec![ColumnarValue::Array(payload_builder.finish().into_array())]
            }
            UnaryScenario::AllFloat => {
                let mut payload_builder = TypedValueArrayElementBuilder::default();
                for i in 0..8192 {
                    payload_builder.append_float(Float::from(i as i16)).unwrap();
                }
                vec![ColumnarValue::Array(payload_builder.finish().into_array())]
            }
            UnaryScenario::AllString => {
                let mut payload_builder = TypedValueArrayElementBuilder::default();
                for i in 0..8192 {
                    payload_builder
                        .append_string(format!("String number {i}").as_str(), None)
                        .unwrap();
                }
                vec![ColumnarValue::Array(payload_builder.finish().into_array())]
            }
        }
    }
}

fn bench_all(c: &mut Criterion) {
    let encodings = RdfFusionEncodings::new(
        PLAIN_TERM_ENCODING,
        TYPED_VALUE_ENCODING,
        None,
        SORTABLE_TERM_ENCODING,
    );
    let registry = DefaultRdfFusionFunctionRegistry::new(encodings);

    let runs = HashMap::from([
        (
            BuiltinName::IsIri,
            vec![UnaryScenario::AllNamedNodes, UnaryScenario::Mixed],
        ),
        (BuiltinName::IsLiteral, vec![UnaryScenario::Mixed]),
        (BuiltinName::IsNumeric, vec![UnaryScenario::Mixed]),
        (
            BuiltinName::IsBlank,
            vec![UnaryScenario::Mixed, UnaryScenario::AllBlank],
        ),
        (
            BuiltinName::Str,
            vec![
                UnaryScenario::Mixed,
                UnaryScenario::AllBlank,
                UnaryScenario::AllNamedNodes,
                UnaryScenario::AllString,
            ],
        ),
        (
            BuiltinName::CastFloat,
            vec![UnaryScenario::Mixed, UnaryScenario::AllInt],
        ),
        (BuiltinName::CastBoolean, vec![UnaryScenario::Mixed]),
        (
            BuiltinName::CastInteger,
            vec![UnaryScenario::Mixed, UnaryScenario::AllFloat],
        ),
        (BuiltinName::CastString, vec![UnaryScenario::Mixed]),
        (BuiltinName::CastDateTime, vec![UnaryScenario::Mixed]),
    ]);

    for (my_built_in, scenarios) in runs {
        let implementation = registry.udf(&FunctionName::Builtin(my_built_in)).unwrap();

        for scenario in scenarios {
            bench_unary_function(c, &implementation, scenario);
        }
    }
}

/// Runs a single `scenario` against the `function` to bench.
fn bench_unary_function(
    c: &mut Criterion,
    function: &ScalarUDF,
    scenario: UnaryScenario,
) {
    let args = scenario.create_args();
    let options = Arc::new(ConfigOptions::default());

    let input_field =
        Arc::new(Field::new("input", TYPED_VALUE_ENCODING.data_type(), true));
    let return_field =
        Arc::new(Field::new("result", TYPED_VALUE_ENCODING.data_type(), true));

    let name = format!("{}_{scenario:?}", function.name());
    c.bench_function(&name, |b| {
        b.iter(|| {
            let args = ScalarFunctionArgs {
                args: args.clone(),
                arg_fields: vec![input_field.clone()],
                number_rows: 8192,
                return_field: return_field.clone(),
                config_options: options.clone(),
            };
            function.invoke_with_args(args).unwrap();
        });
    });
}

criterion_group!(scalar, bench_all);
criterion_main!(scalar);
