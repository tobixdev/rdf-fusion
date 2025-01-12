use crate::decoded::model::DEC_TYPE_TERM;
use crate::decoded::DecRdfTermBuilder;
use crate::encoded::ENC_TYPE_TERM;
use crate::{encoded, DFResult};
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::{Float32Type, Float64Type, Int32Type, Int64Type};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility};
use encoded::{
    ENC_TYPE_ID_BLANK_NODE, ENC_TYPE_ID_BOOLEAN, ENC_TYPE_ID_FLOAT32, ENC_TYPE_ID_FLOAT64,
    ENC_TYPE_ID_INT, ENC_TYPE_ID_INTEGER, ENC_TYPE_ID_NAMED_NODE, ENC_TYPE_ID_STRING,
    ENC_TYPE_ID_TYPED_LITERAL,
};
use oxrdf::vocab::xsd;
use std::sync::Arc;

// TODO: Maybe this should be a logical operator that decodes all outputs at the end
pub const ENC_DECODE: &str = "enc_decode";

pub fn create_enc_decode() -> ScalarUDF {
    create_udf(
        ENC_DECODE,
        vec![ENC_TYPE_TERM.clone()],
        DEC_TYPE_TERM.clone(),
        Volatility::Immutable,
        Arc::new(batch_enc_decode),
    )
}

fn batch_enc_decode(args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    match args {
        [ColumnarValue::Array(arg)] => batch_enc_decode_array(arg.clone()),
        [ColumnarValue::Scalar(arg)] => batch_enc_decode_array(arg.to_array()?),
        _ => Err(DataFusionError::Execution(String::from(
            "Unexpected type combination.",
        ))),
    }
}

fn batch_enc_decode_array(array: ArrayRef) -> DFResult<ColumnarValue> {
    let mut rdf_term_builder = DecRdfTermBuilder::new();
    let enc_array = array.as_union();

    for i in 0..enc_array.len() {
        let type_id = enc_array.type_id(i);
        let value_idx = enc_array.value_offset(i);

        match type_id {
            ENC_TYPE_ID_NAMED_NODE => {
                let named_nodes = enc_array.child(type_id).as_string::<i32>();
                rdf_term_builder.append_named_node(named_nodes.value(value_idx))?
            }
            ENC_TYPE_ID_BLANK_NODE => {
                let blank_nodes = enc_array.child(type_id).as_string::<i32>();
                rdf_term_builder.append_blank_node(blank_nodes.value(value_idx))?
            }
            ENC_TYPE_ID_STRING => {
                let strings = enc_array.child(type_id).as_struct();
                let values = strings.column_by_name("value").unwrap().as_string::<i32>();
                let languages = strings
                    .column_by_name("language")
                    .unwrap()
                    .as_string::<i32>();

                match languages.is_null(value_idx) {
                    true => rdf_term_builder.append_string(values.value(value_idx), None)?,
                    false => rdf_term_builder
                        .append_string(values.value(value_idx), Some(languages.value(value_idx)))?,
                }
            }
            ENC_TYPE_ID_BOOLEAN => {
                let booleans = enc_array.child(type_id).as_boolean();
                match booleans.value(value_idx) {
                    true => rdf_term_builder.append_typed_literal("true", xsd::BOOLEAN.as_str())?,
                    false => {
                        rdf_term_builder.append_typed_literal("false", xsd::BOOLEAN.as_str())?
                    }
                }
            }
            ENC_TYPE_ID_FLOAT32 => {
                let floats = enc_array.child(type_id).as_primitive::<Float32Type>();
                let formatted = floats.value(value_idx).to_string();
                rdf_term_builder.append_typed_literal(&formatted, xsd::FLOAT.as_str())?;
            }
            ENC_TYPE_ID_FLOAT64 => {
                let doubles = enc_array.child(type_id).as_primitive::<Float64Type>();
                let formatted = doubles.value(value_idx).to_string();
                rdf_term_builder.append_typed_literal(&formatted, xsd::DOUBLE.as_str())?;
            }
            ENC_TYPE_ID_INT => {
                let ints = enc_array.child(type_id).as_primitive::<Int32Type>();
                let formatted = ints.value(value_idx).to_string();
                rdf_term_builder.append_typed_literal(&formatted, xsd::INT.as_str())?;
            }
            ENC_TYPE_ID_INTEGER => {
                let integers = enc_array.child(type_id).as_primitive::<Int64Type>();
                let formatted = integers.value(value_idx).to_string();
                rdf_term_builder.append_typed_literal(&formatted, xsd::INTEGER.as_str())?;
            }
            ENC_TYPE_ID_TYPED_LITERAL => {
                let typed_literals = enc_array.child(type_id).as_struct();
                let values = typed_literals
                    .column_by_name("value")
                    .unwrap()
                    .as_string::<i32>();
                let types = typed_literals
                    .column_by_name("datatype")
                    .unwrap()
                    .as_string::<i32>();
                rdf_term_builder
                    .append_typed_literal(values.value(value_idx), types.value(value_idx))?
            }
            _ => unreachable!("Unexpected type id."),
        }
    }

    Ok(ColumnarValue::Array(Arc::new(rdf_term_builder.finish()?)))
}
