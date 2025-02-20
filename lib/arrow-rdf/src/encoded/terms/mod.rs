use crate::encoded::terms::is_iri::EncIsIri;
use crate::encoded::terms::str::EncStr;
use datafusion::logical_expr::ScalarUDF;
use once_cell::unsync::Lazy;

mod is_iri;
mod str;

pub const ENC_IS_IRI: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncIsIri::new()));
pub const ENC_STR: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncStr::new()));
