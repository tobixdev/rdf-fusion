use crate::encoded::terms::bnode::{EncBNodeNullary, EncBNodeUnary};
use crate::encoded::terms::datatype::EncDatatype;
use crate::encoded::terms::iri::EncIri;
use crate::encoded::terms::is_blank::EncIsBlank;
use crate::encoded::terms::is_iri::EncIsIri;
use crate::encoded::terms::is_literal::EncIsLiteral;
use crate::encoded::terms::is_numeric::EncIsNumeric;
use crate::encoded::terms::lang::EncLang;
use crate::encoded::terms::str::EncStr;
use crate::encoded::terms::strdt::EncStrDt;
use crate::encoded::terms::strlang::EncStrLang;
use crate::encoded::terms::struuid::EncStrUuid;
use crate::encoded::terms::uuid::EncUuid;
use datafusion::logical_expr::ScalarUDF;
use once_cell::unsync::Lazy;
use oxiri::Iri;

mod bnode;
mod datatype;
mod iri;
mod is_blank;
mod is_iri;
mod is_literal;
mod is_numeric;
mod lang;
mod str;
mod strdt;
mod strlang;
mod struuid;
mod uuid;

pub const ENC_DATATYPE: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncDatatype::new()));
pub fn enc_iri(base_iri: Option<Iri<String>>) -> ScalarUDF {
    ScalarUDF::from(EncIri::new(base_iri))
}
pub const ENC_IS_IRI: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncIsIri::new()));
pub const ENC_IS_BLANK: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncIsBlank::new()));
pub const ENC_IS_LITERAL: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncIsLiteral::new()));
pub const ENC_IS_NUMERIC: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncIsNumeric::new()));
pub const ENC_STR: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncStr::new()));
pub const ENC_LANG: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncLang::new()));
pub const ENC_BNODE_NULLARY: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncBNodeNullary::new()));
pub const ENC_BNODE_UNARY: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncBNodeUnary::new()));
pub const ENC_STRDT: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncStrDt::new()));
pub const ENC_STRLANG: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncStrLang::new()));
pub const ENC_UUID: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncUuid::new()));
pub const ENC_STRUUID: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncStrUuid::new()));
