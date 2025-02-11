use crate::encoded::udfs::as_native_boolean::EncAsNativeBoolean;
use crate::encoded::udfs::decode::create_enc_decode;
use datafusion::logical_expr::ScalarUDF;
use once_cell::unsync::Lazy;

mod as_native_boolean;
mod binary_dispatch;
mod cmp;
mod decode;
mod effective_boolean_value;
mod not;
mod result_collector;
mod unary_dispatch;

use crate::encoded::udfs::cmp::{
    EncEq, EncGreaterOrEqual, EncGreaterThan, EncLessOrEqual, EncLessThan, EncSameTerm,
};
use crate::encoded::udfs::effective_boolean_value::EncEffectiveBooleanValue;
use crate::encoded::udfs::not::EncNot;

// Unary
pub const ENC_AS_NATIVE_BOOLEAN: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncAsNativeBoolean::new()));
pub const ENC_EFFECTIVE_BOOLEAN_VALUE: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncEffectiveBooleanValue::new()));
pub const ENC_NOT: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncNot::new()));

// Binary Comparisons
pub const ENC_EQ: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncEq::new()));
pub const ENC_GREATER_THAN: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncGreaterThan::new()));
pub const ENC_GREATER_OR_EQUAL: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncGreaterOrEqual::new()));
pub const ENC_LESS_THAN: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncLessThan::new()));
pub const ENC_LESS_OR_EQUAL: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncLessOrEqual::new()));
pub const ENC_SAME_TERM: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncSameTerm::new()));

// Decoding
pub const ENC_DECODE: Lazy<ScalarUDF> = Lazy::new(|| create_enc_decode());
