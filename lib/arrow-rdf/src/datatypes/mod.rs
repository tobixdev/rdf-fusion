mod rdf_blank_node;
mod rdf_language_string;
mod rdf_named_node;
mod rdf_simple_literal;
mod rdf_string_literal;
mod rdf_term;
mod rdf_typed_literal;
mod xsd_boolean;
mod xsd_decimal;
mod xsd_double;
mod xsd_float;
mod xsd_int;
mod xsd_integer;
mod xsd_numeric;

type DFResult<T> = Result<T, DataFusionError>;

pub use self::xsd_boolean::XsdBoolean;
pub use self::xsd_decimal::{ParseDecimalError, TooLargeForDecimalError, XsdDecimal};
pub use self::xsd_double::XsdDouble;
pub use self::xsd_float::XsdFloat;
pub use self::xsd_integer::{TooLargeForIntegerError, XsdInteger};
use datafusion::arrow::array::{Array, UnionArray};
use datafusion::common::ScalarValue;
use datafusion::error::DataFusionError;
pub use rdf_blank_node::RdfBlankNode;
pub use rdf_language_string::RdfLanguageString;
pub use rdf_named_node::RdfNamedNode;
pub use rdf_simple_literal::RdfSimpleLiteral;
pub use rdf_string_literal::CompatibleStringArgs;
pub use rdf_string_literal::RdfStringLiteral;
pub use rdf_term::RdfTerm;
pub use rdf_typed_literal::RdfTypedLiteral;
pub use xsd_int::XsdInt;
pub use xsd_numeric::XsdNumeric;
pub use xsd_numeric::XsdNumericPair;

pub trait RdfValue<'data> {
    fn from_term(term: RdfTerm<'data>) -> DFResult<Self>
    where
        Self: Sized;

    fn from_enc_scalar(scalar: &'data ScalarValue) -> DFResult<Self>
    where
        Self: Sized;

    fn from_enc_array(array: &'data UnionArray, index: usize) -> DFResult<Self>
    where
        Self: Sized;
}

/// Each RdfValue implementation can be wrapped in an option that explicitly handles unbound or
/// erroneous values.
///
/// Some Functions handle Unbound/Errors in a special way (e.g., && and ||) and thus need access to
/// a wrapper that represents Unbound/Error explicitly.
impl<'value, TValue> RdfValue<'value> for Option<TValue>
where
    TValue: RdfValue<'value>,
{
    fn from_term(term: RdfTerm<'value>) -> DFResult<Self>
    where
        Self: Sized,
    {
        TValue::from_term(term).map(Some)
    }

    fn from_enc_scalar(scalar: &'value ScalarValue) -> DFResult<Self>
    where
        Self: Sized,
    {
        if scalar.is_null() {
            Ok(None)
        } else {
            TValue::from_enc_scalar(scalar).map(Some)
        }
    }

    fn from_enc_array(array: &'value UnionArray, index: usize) -> DFResult<Self>
    where
        Self: Sized,
    {
        if array.is_null(index) {
            Ok(None)
        } else {
            TValue::from_enc_array(array, index).map(Some)
        }
    }
}
