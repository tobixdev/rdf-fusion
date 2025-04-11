use crate::encoded::effective_boolean_value::EncEffectiveBooleanValue;
use crate::encoded::logical::and::EncAnd;
use crate::encoded::logical::or::EncOr;
use datafusion::logical_expr::ScalarUDF;
use once_cell::sync::Lazy;

mod and;
pub mod effective_boolean_value;
mod or;

pub const ENC_AND: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncAnd::new()));
pub const ENC_EFFECTIVE_BOOLEAN_VALUE: once_cell::unsync::Lazy<ScalarUDF> =
    once_cell::unsync::Lazy::new(|| ScalarUDF::from(EncEffectiveBooleanValue::new()));
pub const ENC_OR: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncOr::new()));
