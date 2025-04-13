use crate::encoded::as_native_boolean::EncAsNativeBoolean;
use crate::encoded::with_struct_encoding::EncWithSortableEncoding;
use crate::encoded::encoding::boolean_as_rdf_term::EncBooleanAsRdfTerm;
use crate::encoded::encoding::int64_as_rdf_term::EncInt64AsRdfTerm;
use datafusion::logical_expr::ScalarUDF;
use once_cell::sync::Lazy;

pub mod as_native_boolean;
pub mod with_struct_encoding;
mod boolean_as_rdf_term;
mod int64_as_rdf_term;

pub const ENC_AS_NATIVE_BOOLEAN: once_cell::unsync::Lazy<ScalarUDF> =
    once_cell::unsync::Lazy::new(|| ScalarUDF::from(EncAsNativeBoolean::new()));
pub const ENC_WITH_SORTABLE_ENCODING: once_cell::unsync::Lazy<ScalarUDF> =
    once_cell::unsync::Lazy::new(|| ScalarUDF::from(EncWithSortableEncoding::new()));
pub const ENC_BOOLEAN_AS_RDF_TERM: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncBooleanAsRdfTerm::new()));
pub const ENC_INT64_AS_RDF_TERM: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncInt64AsRdfTerm::new()));
