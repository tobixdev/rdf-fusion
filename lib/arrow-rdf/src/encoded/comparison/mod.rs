use crate::encoded::comparison::eq::EncEq;
use crate::encoded::comparison::generic::{
    EncGreaterOrEqual, EncGreaterThan, EncLessOrEqual, EncLessThan,
};
use crate::encoded::comparison::same_term::EncSameTerm;
use datafusion::logical_expr::ScalarUDF;
use once_cell::unsync::Lazy;

mod eq;
mod generic;
mod same_term;

pub const ENC_EQ: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncEq::new()));
pub const ENC_GREATER_THAN: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncGreaterThan::new()));
pub const ENC_GREATER_OR_EQUAL: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncGreaterOrEqual::new()));
pub const ENC_LESS_THAN: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncLessThan::new()));
pub const ENC_LESS_OR_EQUAL: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncLessOrEqual::new()));
pub const ENC_SAME_TERM: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncSameTerm::new()));
