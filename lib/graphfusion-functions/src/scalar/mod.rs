use datafusion::logical_expr::ColumnarValue;
use graphfusion_encoding::{as_term_value_array, FromArrow};
use model::{ThinError, ThinResult};

mod binary;
mod n_ary;
mod nullary;
mod quaternary;
mod ternary;
mod unary;

fn borrow_value<'data, TValue>(value: &'data ColumnarValue, index: usize) -> ThinResult<TValue>
where
    TValue: FromArrow<'data>,
{
    // TODO: Improve this. Maybe a custom iterator?
    match value {
        ColumnarValue::Array(arr) => {
            let arr = as_term_value_array(arr.as_ref())
                .map_err(|_| ThinError::InternalError("Expected an array of EncTerm."))?;
            TValue::from_array(arr, index)
        }
        ColumnarValue::Scalar(scalar) => TValue::from_scalar(scalar),
    }
}
