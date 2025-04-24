use crate::encoded::as_native_boolean::EncAsNativeBoolean;
use crate::encoded::encoding::boolean_as_rdf_term::EncBooleanAsRdfTerm;
use crate::encoded::encoding::int64_as_rdf_term::EncInt64AsRdfTerm;
use crate::encoded::with_struct_encoding::EncWithSortableEncoding;
use datafusion::logical_expr::ScalarUDF;
use std::sync::LazyLock;

pub mod as_native_boolean;
mod boolean_as_rdf_term;
mod int64_as_rdf_term;
pub mod with_struct_encoding;

pub const ENC_AS_NATIVE_BOOLEAN: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::from(EncAsNativeBoolean::new()));
pub const ENC_WITH_SORTABLE_ENCODING: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::from(EncWithSortableEncoding::new()));
pub const ENC_BOOLEAN_AS_RDF_TERM: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::from(EncBooleanAsRdfTerm::new()));
pub const ENC_INT64_AS_RDF_TERM: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::from(EncInt64AsRdfTerm::new()));
