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
use datafusion::arrow::array::UnionArray;
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
