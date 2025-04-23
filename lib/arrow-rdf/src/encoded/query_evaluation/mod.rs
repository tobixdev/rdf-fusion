mod is_compatible;

use datafusion::logical_expr::ScalarUDF;
pub use is_compatible::*;
use once_cell::sync::Lazy;

pub const ENC_IS_COMPATIBLE: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncIsCompatible::new()));
