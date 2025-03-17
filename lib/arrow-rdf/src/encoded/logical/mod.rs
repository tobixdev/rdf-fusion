use datafusion::logical_expr::ScalarUDF;
use once_cell::sync::Lazy;
use crate::encoded::logical::and::EncAnd;
use crate::encoded::logical::or::EncOr;

mod and;
mod or;

pub const ENC_AND: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncAnd::new()));
pub const ENC_OR: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncOr::new()));