use crate::encoded::udfs::as_native_boolean::create_enc_as_native_boolean;
use crate::encoded::udfs::decode::create_enc_decode;
use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::SessionContext;

mod as_native_boolean;
mod cmp;
mod decode;
#[macro_use]
mod macros;
mod binary_dispatch;
mod result_collector;

use crate::encoded::udfs::cmp::EncEq;
pub use as_native_boolean::ENC_AS_NATIVE_BOOLEAN;
pub use cmp::ENC_EQ;
pub use decode::ENC_DECODE;

pub fn register_rdf_term_udfs(session_context: &SessionContext) {
    session_context.register_udf(ScalarUDF::from(EncEq::new()));
    session_context.register_udf(create_enc_as_native_boolean());
    session_context.register_udf(create_enc_decode());
}
