use crate::encoded::encoding::boolean_as_rdf_term::EncBooleanAsRdfTerm;
use datafusion::logical_expr::ScalarUDF;
use once_cell::sync::Lazy;

mod boolean_as_rdf_term;

pub const ENC_BOOLEAN_AS_RDF_TERM: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncBooleanAsRdfTerm::new()));
