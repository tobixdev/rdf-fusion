use codspeed_criterion_compat::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::datatypes::Field;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use rdf_fusion_extensions::functions::{
    BuiltinName, FunctionName, RdfFusionFunctionArgs, RdfFusionFunctionRegistry,
};
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
use rdf_fusion_encoding::typed_value::{TYPED_VALUE_ENCODING, TypedValueArrayBuilder};
use rdf_fusion_encoding::{EncodingArray, RdfFusionEncodings, TermEncoding};
use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
use rdf_fusion_model::Integer;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
enum BinaryScenario {
    AllInt,
}

impl BinaryScenario {
    fn create_args(&self) -> Vec<ColumnarValue> {
        match self {
            BinaryScenario::AllInt => {
                let mut left_builder = TypedValueArrayBuilder::default();
                let mut right_builder = TypedValueArrayBuilder::default();
                for i in 0..8192 {
                    match i % 3 {
                        1 => {
                            left_builder.append_integer(Integer::from(i)).unwrap();
                            right_builder.append_integer(Integer::from(i)).unwrap();
                        }
                        2 => {
                            left_builder.append_integer(Integer::from(i)).unwrap();
                            right_builder.append_integer(Integer::from(i + 1)).unwrap();
                        }
                        _ => {
                            left_builder.append_integer(Integer::from(i + 1)).unwrap();
                            right_builder.append_integer(Integer::from(i)).unwrap();
                        }
                    }
                }
                vec![
                    ColumnarValue::Array(left_builder.finish().into_array()),
                    ColumnarValue::Array(right_builder.finish().into_array()),
                ]
            }
        }
    }
}

//TODO: write run for BuiltinName::SameTerm; add other scenarios
fn bench_all_binary(c: &mut Criterion) {
    let encodings = RdfFusionEncodings::new(
        PLAIN_TERM_ENCODING,
        TYPED_VALUE_ENCODING,
        None,
        SORTABLE_TERM_ENCODING,
    );
    let registry = DefaultRdfFusionFunctionRegistry::new(encodings);

    let runs = HashMap::from([
        (BuiltinName::Equal, vec![BinaryScenario::AllInt]),
        (BuiltinName::GreaterOrEqual, vec![BinaryScenario::AllInt]),
        (BuiltinName::GreaterThan, vec![BinaryScenario::AllInt]),
        (BuiltinName::LessOrEqual, vec![BinaryScenario::AllInt]),
        (BuiltinName::LessThan, vec![BinaryScenario::AllInt]),
    ]);

    for (my_built_in, scenarios) in runs {
        let implementation = registry
            .create_udf(
                FunctionName::Builtin(my_built_in),
                RdfFusionFunctionArgs::empty(),
            )
            .unwrap();

        for scenario in scenarios {
            bench_binary_function(c, &implementation, scenario);
        }
    }
}

/// Runs a single `scenario` against the `function` to bench.
fn bench_binary_function(
    c: &mut Criterion,
    function: &ScalarUDF,
    scenario: BinaryScenario,
) {
    let args = scenario.create_args();
    let options = Arc::new(ConfigOptions::default());

    let input_field_left =
        Arc::new(Field::new("left", TYPED_VALUE_ENCODING.data_type(), true));
    let input_field_right =
        Arc::new(Field::new("right", TYPED_VALUE_ENCODING.data_type(), true));
    let return_field =
        Arc::new(Field::new("result", TYPED_VALUE_ENCODING.data_type(), true));

    /* code used only for testing purposes (remove before official launch)
    // determine correct return type of UDF
    let return_type = function
        .return_type(&[TYPED_VALUE_ENCODING.data_type(), TYPED_VALUE_ENCODING.data_type()])
        .expect("cannot resolve return type");
    let return_field = Arc::new(Field::new("result", return_type, true));*/

    let name = format!("{}_{scenario:?}", function.name());
    c.bench_function(&name, |b| {
        b.iter(|| {
            let args = ScalarFunctionArgs {
                args: args.clone(),
                arg_fields: vec![input_field_left.clone(), input_field_right.clone()],
                number_rows: 8192,
                return_field: return_field.clone(),
                config_options: options.clone(),
            };
            function.invoke_with_args(args).unwrap();
        });
    });
}

criterion_group!(scalar_binary, bench_all_binary);
criterion_main!(scalar_binary);
