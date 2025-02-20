use crate::encoded::terms::str::EncStr;
use datafusion::logical_expr::ScalarUDF;
use once_cell::unsync::Lazy;

mod str;

pub const ENC_STR: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncStr::new()));
