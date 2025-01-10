use crate::encoded::udfs::eq::RDF_TERM_EQ;
use datafusion::prelude::SessionContext;

mod eq;

pub fn register_rdf_term_udfs(session_context: &SessionContext) {
    session_context.register_udf(RDF_TERM_EQ.clone());
}
