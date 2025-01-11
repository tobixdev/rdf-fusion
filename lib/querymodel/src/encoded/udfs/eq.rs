use crate::encoded::TYPE_TERM;
use crate::DFResult;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::{create_udf, ColumnarValue, ScalarUDF, Signature, Volatility};
use std::sync::Arc;

#[derive(Debug)]
struct RdfTermEq {
    signature: Signature,
}

pub fn create_rdf_term_eq_udf() -> ScalarUDF {
    create_udf(
        "rdf_term_eq",
        vec![TYPE_TERM.clone(), TYPE_TERM.clone()],
        DataType::Boolean,
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

fn batch_rdf_term_eq_array_array(lhs: ArrayRef, rhs: ArrayRef) -> DFResult<ColumnarValue> {
    Err(DataFusionError::NotImplemented(
        "batch_rdf_term_eq_array_array".to_string(),
    ))
}

fn batch_rdf_term_eq_scalar_array(lhs: ScalarValue, rhs_: ArrayRef) -> DFResult<ColumnarValue> {
    Err(DataFusionError::NotImplemented(
        "batch_rdf_term_eq_scalar_array".to_string(),
    ))
}

fn batch_rdf_term_eq_scalar_scalar(lhs: ScalarValue, rhs: ScalarValue) -> DFResult<ColumnarValue> {
    Err(DataFusionError::NotImplemented(
        "batch_rdf_term_eq_scalar_scalar".to_string(),
    ))
}
