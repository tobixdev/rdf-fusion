use crate::encoded::cast::cast_to_rdf_term;
use crate::encoded::{enc_idx_to_field_name, enc_is_nested_rdf_term, ENC_TYPE_TERM};
use crate::{as_rdf_term_array, DFResult};
use datafusion::arrow::array::{ArrayRef, Scalar};
use datafusion::arrow::compute::union_extract;
use datafusion::common::{not_impl_datafusion_err, DataFusionError, ScalarValue};
use datafusion::logical_expr::{create_udf, ColumnarValue, ScalarUDF, Volatility};
use datafusion::physical_expr_common::datum::compare_with_eq;
use std::sync::Arc;

pub const ENC_EQ: &str = "enc_eq";

pub fn create_enc_eq() -> ScalarUDF {
    create_udf(
        ENC_EQ,
        vec![ENC_TYPE_TERM.clone(), ENC_TYPE_TERM.clone()],
        ENC_TYPE_TERM.clone(),
        Volatility::Immutable,
        Arc::new(batch_rdf_term_eq),
    )
}

fn batch_rdf_term_eq(args: &[ColumnarValue]) -> DFResult<ColumnarValue> {
    match args {
        [ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)] => {
            batch_rdf_term_eq_array_array(lhs.clone(), rhs.clone())
        }
        [ColumnarValue::Scalar(lhs), ColumnarValue::Array(rhs)] => {
            batch_rdf_term_eq_scalar_array(lhs.clone(), rhs.clone())
        }
        [ColumnarValue::Array(lhs), ColumnarValue::Scalar(rhs)] => {
            batch_rdf_term_eq_scalar_array(rhs.clone(), lhs.clone())
        }
        [ColumnarValue::Scalar(lhs), ColumnarValue::Scalar(rhs)] => {
            batch_rdf_term_eq_scalar_scalar(lhs.clone(), rhs.clone())
        }
        _ => Err(DataFusionError::Execution(String::from(
            "Unexpected type combination.",
        ))),
    }
}

fn batch_rdf_term_eq_array_array(_lhs: ArrayRef, _rhs: ArrayRef) -> DFResult<ColumnarValue> {
    Err(DataFusionError::NotImplemented(
        "batch_rdf_term_eq_array_array".to_string(),
    ))
}

fn batch_rdf_term_eq_scalar_array(lhs: ScalarValue, rhs: ArrayRef) -> DFResult<ColumnarValue> {
    let ScalarValue::Union(element, _, _) = lhs else {
        return Err(DataFusionError::Internal(
            "Unexpected type for scalar.".to_string(),
        ));
    };

    match element {
        None => Err(not_impl_datafusion_err!(
            "batch_rdf_term_eq_scalar_array: None Case"
        )),
        Some((idx, element)) => {
            let is_nested = enc_is_nested_rdf_term(idx);
            let rdf_term = as_rdf_term_array(&rhs)?;
            let rhs = union_extract(rdf_term, enc_idx_to_field_name(idx).as_str())?;

            let booleans = compare_with_eq(&Scalar::new(element.to_array()?), &rhs, is_nested)?;
            Ok(ColumnarValue::Array(cast_to_rdf_term(booleans)?))
        }
    }
}

fn batch_rdf_term_eq_scalar_scalar(
    _lhs: ScalarValue,
    _rhs: ScalarValue,
) -> DFResult<ColumnarValue> {
    Err(not_impl_datafusion_err!("batch_rdf_term_eq_scalar_scalar"))
}
