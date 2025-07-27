use crate::typed_value::TypedValueEncoding;
use datafusion::arrow::error::ArrowError;

mod encoding;
mod encoding_name;
mod encodings;
pub mod object_id;
pub mod plain_term;
mod quad_storage_encoding;
mod scalar_encoder;
pub mod sortable_term;
pub mod typed_value;

pub use encoding::*;
pub use encoding_name::*;
pub use encodings::*;
pub use quad_storage_encoding::*;
pub use scalar_encoder::ScalarEncoder;

type AResult<T> = Result<T, ArrowError>;
