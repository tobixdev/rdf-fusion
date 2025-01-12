use crate::encoded::udfs::as_native_boolean::create_enc_as_native_boolean;
use crate::encoded::udfs::decode::create_enc_decode;
use crate::encoded::udfs::eq::create_enc_eq;
use datafusion::prelude::SessionContext;

mod as_native_boolean;
mod decode;
mod eq;

pub use as_native_boolean::ENC_AS_NATIVE_BOOLEAN;
pub use decode::ENC_DECODE;
pub use eq::ENC_EQ;

pub fn register_rdf_term_udfs(session_context: &SessionContext) {
    session_context.register_udf(create_enc_eq());
    session_context.register_udf(create_enc_as_native_boolean());
    session_context.register_udf(create_enc_decode());
}
