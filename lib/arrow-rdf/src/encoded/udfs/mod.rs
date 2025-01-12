use crate::encoded::udfs::as_bool::create_rdf_term_as_boolean_udf;
use crate::encoded::udfs::eq::create_rdf_term_eq_udf;
use datafusion::prelude::SessionContext;

mod as_bool;
mod eq;

pub fn register_rdf_term_udfs(session_context: &SessionContext) {
    session_context.register_udf(create_rdf_term_eq_udf());
    session_context.register_udf(create_rdf_term_as_boolean_udf());
}
