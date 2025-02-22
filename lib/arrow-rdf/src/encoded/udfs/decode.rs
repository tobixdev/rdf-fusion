use crate::decoded::model::DecTerm;
use crate::decoded::DecRdfTermBuilder;
use crate::encoded::{EncTerm, EncTermField};
use crate::{DFResult, RDF_DECIMAL_PRECISION, RDF_DECIMAL_SCALE};
use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::{
    Decimal128Type, DecimalType, Float32Type, Float64Type, Int32Type, Int64Type,
};
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility};
use oxrdf::vocab::xsd;
use std::sync::Arc;

pub const ENC_DECODE: &str = "enc_decode";

pub fn create_enc_decode() -> ScalarUDF {
    create_udf(
        ENC_DECODE,
        vec![EncTerm::term_type()],
        DecTerm::term_type(),
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
        let type_id: EncTermField = enc_array.type_id(i).try_into()?;
        let value_idx = enc_array.value_offset(i);

        match type_id {
            EncTermField::NamedNode => {
                let named_nodes = enc_array.child(type_id.type_id()).as_string::<i32>();
                rdf_term_builder.append_named_node(named_nodes.value(value_idx))?
            }
            EncTermField::BlankNode => {
                let blank_nodes = enc_array.child(type_id.type_id()).as_string::<i32>();
                rdf_term_builder.append_blank_node(blank_nodes.value(value_idx))?
            }
            EncTermField::String => {
                let strings = enc_array.child(type_id.type_id()).as_struct();
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
            EncTermField::Boolean => {
                let booleans = enc_array.child(type_id.type_id()).as_boolean();
                match booleans.value(value_idx) {
                    true => rdf_term_builder.append_typed_literal("true", xsd::BOOLEAN.as_str())?,
                    false => {
                        rdf_term_builder.append_typed_literal("false", xsd::BOOLEAN.as_str())?
                    }
                }
            }
            EncTermField::Float32 => {
                let floats = enc_array
                    .child(type_id.type_id())
                    .as_primitive::<Float32Type>();
                let formatted = floats.value(value_idx).to_string();
                rdf_term_builder.append_typed_literal(&formatted, xsd::FLOAT.as_str())?;
            }
            EncTermField::Float64 => {
                let doubles = enc_array
                    .child(type_id.type_id())
                    .as_primitive::<Float64Type>();
                let formatted = doubles.value(value_idx).to_string();
                rdf_term_builder.append_typed_literal(&formatted, xsd::DOUBLE.as_str())?;
            }
            EncTermField::Decimal => {
                let decimals = enc_array
                    .child(type_id.type_id())
                    .as_primitive::<Decimal128Type>();
                let formatted = Decimal128Type::format_decimal(
                    decimals.value(value_idx),
                    RDF_DECIMAL_PRECISION,
                    RDF_DECIMAL_SCALE,
                );
                rdf_term_builder.append_typed_literal(&formatted, xsd::DECIMAL.as_str())?;
            }
            EncTermField::Int => {
                let ints = enc_array
                    .child(type_id.type_id())
                    .as_primitive::<Int32Type>();
                let formatted = ints.value(value_idx).to_string();
                rdf_term_builder.append_typed_literal(&formatted, xsd::INT.as_str())?;
            }
            EncTermField::Integer => {
                let integers = enc_array
                    .child(type_id.type_id())
                    .as_primitive::<Int64Type>();
                let formatted = integers.value(value_idx).to_string();
                rdf_term_builder.append_typed_literal(&formatted, xsd::INTEGER.as_str())?;
            }
            EncTermField::TypedLiteral => {
                let typed_literals = enc_array.child(type_id.type_id()).as_struct();
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
            EncTermField::Null => {
                rdf_term_builder.append_null()?;
            }
        }
    }

    Ok(ColumnarValue::Array(Arc::new(rdf_term_builder.finish()?)))
}
