use crate::encoded::conversion::as_decimal::EncAsDecimal;
use crate::encoded::conversion::as_float32::EncAsFloat32;
use crate::encoded::conversion::as_float64::EncAsFloat64;
use crate::encoded::conversion::as_int::EncAsInt;
use crate::encoded::conversion::as_integer::EncAsInteger;
use datafusion::logical_expr::ScalarUDF;
use once_cell::sync::Lazy;

mod as_decimal;
mod as_float32;
mod as_float64;
mod as_int;
mod as_integer;

pub const ENC_AS_DECIMAL: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncAsDecimal::new()));
pub const ENC_AS_INT: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncAsInt::new()));
pub const ENC_AS_INTEGER: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncAsInteger::new()));
pub const ENC_AS_FLOAT32: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncAsFloat32::new()));
pub const ENC_AS_FLOAT64: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncAsFloat64::new()));
