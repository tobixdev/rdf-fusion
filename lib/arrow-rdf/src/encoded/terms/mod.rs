use crate::encoded::terms::is_iri::EncIsIri;
use crate::encoded::terms::str::EncStr;
use datafusion::logical_expr::ScalarUDF;
use once_cell::unsync::Lazy;
use crate::encoded::terms::is_blank::EncIsBlank;
use crate::encoded::terms::is_literal::EncIsLiteral;

mod is_iri;
mod str;
mod is_blank;
mod is_literal;

pub const ENC_IS_IRI: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncIsIri::new()));
pub const ENC_IS_BLANK: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncIsBlank::new()));
pub const ENC_IS_LITERAL: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncIsLiteral::new()));
pub const ENC_STR: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncStr::new()));
