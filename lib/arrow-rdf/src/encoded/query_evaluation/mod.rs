use crate::encoded::query_evaluation::is_compatible::EncIsCompatible;
use datafusion::logical_expr::ScalarUDF;
use once_cell::unsync::Lazy;

mod is_compatible;

pub const ENC_IS_COMPATIBLE: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncIsCompatible::new()));
