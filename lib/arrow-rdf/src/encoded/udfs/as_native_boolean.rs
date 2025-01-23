use crate::encoded::{EncTerm, EncTermField};
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
        vec![EncTerm::term_type()],
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
        .map(|tid| EncTermField::try_from(*tid))
        .zip(arg.offsets().expect("Dense Union").iter())
        .map(|(term_field, offset)| {
            let term_field = term_field?;
            let result = match term_field {
                EncTermField::Boolean => arg
                    .child(term_field.type_id())
                    .as_boolean()
                    .value(*offset as usize),
                _ => false,
            };
            Ok(Some(result))
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    let booleans = BooleanArray::from(booleans_iter);

    Ok(ColumnarValue::Array(Arc::new(booleans)))
}
