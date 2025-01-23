use crate::encoded::udfs::as_native_boolean::create_enc_as_native_boolean;
use crate::encoded::udfs::decode::create_enc_decode;
use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::SessionContext;
use once_cell::unsync::Lazy;

mod as_native_boolean;
mod binary_dispatch;
mod cmp;
mod decode;
mod result_collector;

use crate::encoded::udfs::cmp::{
    EncEq, EncGreaterOrEqual, EncGreaterThan, EncLessOrEqual, EncLessThan,
};

pub const ENC_EQ: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncEq::new()));
pub const ENC_GREATER_THAN: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncGreaterThan::new()));
pub const ENC_GREATER_OR_EQUAL: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncGreaterOrEqual::new()));
pub const ENC_LESS_THAN: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncLessThan::new()));
pub const ENC_LESS_OR_EQUAL: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncLessOrEqual::new()));


pub const ENC_AS_NATIVE_BOOLEAN: Lazy<ScalarUDF> =
    Lazy::new(|| create_enc_as_native_boolean());

pub const ENC_DECODE: Lazy<ScalarUDF> =
    Lazy::new(|| create_enc_decode());

pub fn register_rdf_term_udfs(session_context: &SessionContext) {
    // Binary Comparisons
    session_context.register_udf(ENC_EQ.clone());
    session_context.register_udf(ENC_GREATER_THAN.clone());
    session_context.register_udf(ENC_GREATER_OR_EQUAL.clone());
    session_context.register_udf(ENC_LESS_THAN.clone());
    session_context.register_udf(ENC_LESS_OR_EQUAL.clone());

    session_context.register_udf(ENC_AS_NATIVE_BOOLEAN.clone());
    session_context.register_udf(ENC_DECODE.clone());
}
