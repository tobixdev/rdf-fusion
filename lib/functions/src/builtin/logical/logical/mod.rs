use crate::value_encoding::effective_boolean_value::EncEffectiveBooleanValue;
use crate::value_encoding::logical::and::EncAnd;
use crate::value_encoding::logical::or::EncOr;
use datafusion::logical_expr::ScalarUDF;
use std::sync::LazyLock;

mod and;
pub mod effective_boolean_value;
mod or;

pub static ENC_AND: LazyLock<ScalarUDF> = LazyLock::new(|| ScalarUDF::from(EncAnd::new()));
pub static ENC_EFFECTIVE_BOOLEAN_VALUE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::from(EncEffectiveBooleanValue::new()));
pub static ENC_OR: LazyLock<ScalarUDF> = LazyLock::new(|| ScalarUDF::from(EncOr::new()));
