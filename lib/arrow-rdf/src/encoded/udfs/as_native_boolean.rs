use crate::encoded::{ENC_TYPE_ID_BOOLEAN, ENC_TYPE_TERM};
use crate::{as_rdf_term_array, DFResult};
use datafusion::arrow::array::{ArrayRef, AsArray, BooleanArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility};
use std::sync::Arc;

pub const ENC_AS_NATIVE_BOOLEAN: &str = "enc_as_native_boolean";

pub fn create_enc_as_native_boolean() -> ScalarUDF {
    create_udf(
        ENC_AS_NATIVE_BOOLEAN,
        vec![ENC_TYPE_TERM.clone()],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(batch_enc_as_native_boolean),
    )
}

fn batch_enc_as_native_boolean(args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    match args {
        [ColumnarValue::Array(arg)] => batch_enc_as_native_boolean_array(arg.clone()),
        [ColumnarValue::Scalar(arg)] => batch_enc_as_native_boolean_array(arg.to_array()?),
        _ => Err(DataFusionError::Execution(String::from(
            "Unexpected type combination.",
        ))),
    }
}

fn batch_enc_as_native_boolean_array(arg: ArrayRef) -> DFResult<ColumnarValue> {
    // TODO: Handle other types and maybe there is a better implementation by doing it column-wise
    // TODO: Null handling
    let arg = as_rdf_term_array(&arg)?;

    let booleans_iter = arg
        .type_ids()
        .iter()
        .zip(arg.offsets().expect("Dense Union").iter())
        .map(|(tid, offset)| {
            let result = match tid {
                &ENC_TYPE_ID_BOOLEAN => arg.child(*tid).as_boolean().value(*offset as usize),
                _ => false,
            };
            Some(result)
        });
    let booleans = BooleanArray::from_iter(booleans_iter);

    Ok(ColumnarValue::Array(Arc::new(booleans)))
}
