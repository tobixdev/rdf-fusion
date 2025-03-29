use crate::encoded::udfs::as_native_boolean::EncAsNativeBoolean;
use crate::encoded::udfs::as_struct_encoding::EncAsStructEncoding;
use datafusion::logical_expr::ScalarUDF;
use once_cell::unsync::Lazy;

mod as_native_boolean;
mod as_struct_encoding;
mod effective_boolean_value;

use crate::encoded::udfs::effective_boolean_value::EncEffectiveBooleanValue;

// Unary
pub const ENC_AS_NATIVE_BOOLEAN: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncAsNativeBoolean::new()));
pub const ENC_EFFECTIVE_BOOLEAN_VALUE: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncEffectiveBooleanValue::new()));
pub const ENC_AS_STRUCT_ENCODING: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncAsStructEncoding::new()));
