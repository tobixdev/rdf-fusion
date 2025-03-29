use crate::encoded::udfs::as_native_boolean::EncAsNativeBoolean;
use crate::encoded::udfs::as_rdf_term_sort::EncAsRdfTermSort;
use datafusion::logical_expr::ScalarUDF;
use once_cell::unsync::Lazy;

mod as_native_boolean;
mod as_rdf_term_sort;
mod effective_boolean_value;

use crate::encoded::udfs::effective_boolean_value::EncEffectiveBooleanValue;

// Unary
pub const ENC_AS_NATIVE_BOOLEAN: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncAsNativeBoolean::new()));
pub const ENC_EFFECTIVE_BOOLEAN_VALUE: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncEffectiveBooleanValue::new()));
pub const ENC_AS_RDF_TERM_SORT: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncAsRdfTermSort::new()));
