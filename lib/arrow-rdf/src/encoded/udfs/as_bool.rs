use crate::encoded::{ENC_TYPE_ID_BOOLEAN, ENC_TYPE_TERM};
use crate::{as_rdf_term_array, DFResult};
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{create_udf, ColumnarValue, ScalarUDF, Signature, Volatility};
use std::sync::Arc;

#[derive(Debug)]
struct RdfTermAsBoolean {
    signature: Signature,
}

pub fn create_rdf_term_as_boolean_udf() -> ScalarUDF {
    create_udf(
        "rdf_term_as_boolean",
        vec![ENC_TYPE_TERM.clone()],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(batch_rdf_term_as_boolean),
    )
}

fn batch_rdf_term_as_boolean(args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    match args {
        [ColumnarValue::Array(arg)] => {
            let dt = arg.data_type().to_string();
            batch_rdf_term_as_boolean_array(arg.clone())
        }
        [ColumnarValue::Scalar(arg)] => batch_rdf_term_as_boolean_array(arg.to_array()?),
        _ => Err(DataFusionError::Execution(String::from(
            "Unexpected type combination.",
        ))),
    }
}

fn batch_rdf_term_as_boolean_array(arg: ArrayRef) -> DFResult<ColumnarValue> {
    // TODO: Handle other types
    let arg = as_rdf_term_array(&arg)?;

    let booleans_iter = arg
        .type_ids()
        .iter()
        .map(|t| Some(t == &ENC_TYPE_ID_BOOLEAN));
    let booleans = BooleanArray::from_iter(booleans_iter);

    // TODO: Check if true
    Ok(ColumnarValue::Array(Arc::new(booleans)))
}
