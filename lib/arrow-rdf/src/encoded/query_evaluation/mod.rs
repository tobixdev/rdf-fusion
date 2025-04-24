mod is_compatible;

use datafusion::logical_expr::ScalarUDF;
pub use is_compatible::*;
use std::sync::LazyLock;

pub static ENC_IS_COMPATIBLE: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::from(EncIsCompatible::default()));
