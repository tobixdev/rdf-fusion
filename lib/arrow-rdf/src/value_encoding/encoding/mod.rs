use crate::value_encoding::as_native_boolean::EncAsNativeBoolean;
use crate::value_encoding::encoding::boolean_as_rdf_term::EncBooleanAsRdfTerm;
use crate::value_encoding::encoding::int64_as_rdf_term::EncInt64AsRdfTerm;
use crate::value_encoding::with_struct_encoding::EncWithSortableEncoding;
use datafusion::logical_expr::ScalarUDF;
use std::sync::LazyLock;

pub mod as_native_boolean;
mod boolean_as_rdf_term;
mod int64_as_rdf_term;
pub mod with_struct_encoding;

pub static ENC_AS_NATIVE_BOOLEAN: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::from(EncAsNativeBoolean::new()));
pub static ENC_WITH_SORTABLE_ENCODING: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::from(EncWithSortableEncoding::new()));
pub static ENC_BOOLEAN_AS_RDF_TERM: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::from(EncBooleanAsRdfTerm::new()));
pub static ENC_INT64_AS_RDF_TERM: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::from(EncInt64AsRdfTerm::new()));
