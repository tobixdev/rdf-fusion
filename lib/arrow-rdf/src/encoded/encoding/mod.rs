use crate::encoded::encoding::boolean_as_rdf_term::EncBooleanAsRdfTerm;
use crate::encoded::encoding::int64_as_rdf_term::EncInt64AsRdfTerm;
use datafusion::logical_expr::ScalarUDF;
use once_cell::sync::Lazy;

mod boolean_as_rdf_term;
mod int64_as_rdf_term;

pub const ENC_BOOLEAN_AS_RDF_TERM: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncBooleanAsRdfTerm::new()));
pub const ENC_INT64_AS_RDF_TERM: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncInt64AsRdfTerm::new()));
