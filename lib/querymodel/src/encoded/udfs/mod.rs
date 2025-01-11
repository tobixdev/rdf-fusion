use crate::encoded::udfs::eq::create_rdf_term_eq_udf;
use datafusion::prelude::SessionContext;

mod eq;

pub fn register_rdf_term_udfs(session_context: &SessionContext) {
    session_context.register_udf(create_rdf_term_eq_udf());
}
