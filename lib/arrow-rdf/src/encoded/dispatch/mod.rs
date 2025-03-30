use datafusion::logical_expr::ColumnarValue;
use datamodel::RdfOpResult;
use crate::as_enc_term_array;
use crate::encoded::from_encoded_term::FromEncodedTerm;

mod dispatch_unary;
mod dispatch_binary;
mod dispatch_ternary;
mod dispatch_quaternary;
mod dispatch_n_ary;

pub use dispatch_unary::dispatch_unary;
pub use dispatch_binary::dispatch_binary;
pub use dispatch_ternary::dispatch_ternary;
pub use dispatch_quaternary::dispatch_quaternary;
pub use dispatch_n_ary::dispatch_n_ary;

fn borrow_value<'data, TValue>(value: &'data ColumnarValue, index: usize) -> RdfOpResult<TValue>
where
    TValue: FromEncodedTerm<'data>,
{
    // TODO: Improve this. Maybe a custom iterator?
    match value {
        ColumnarValue::Array(arr) => {
            let arr = as_enc_term_array(arr.as_ref()).expect("Type checked");
            TValue::from_enc_array(arr, index)
        }
        ColumnarValue::Scalar(scalar) => TValue::from_enc_scalar(scalar),
    }
}
