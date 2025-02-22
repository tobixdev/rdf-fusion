use crate::encoded::functional_forms::bound::EncBound;
use datafusion::logical_expr::ScalarUDF;
use once_cell::unsync::Lazy;

mod bound;

pub const ENC_BOUND: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncBound::new()));
