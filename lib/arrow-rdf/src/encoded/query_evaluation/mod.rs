mod is_compatible;

use datafusion::logical_expr::ScalarUDF;
use once_cell::sync::Lazy;
pub use is_compatible::*;

pub const ENC_IS_COMPATIBLE: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncIsCompatible::new()));