use crate::encoding::TermEncoding;
use crate::plain_term::encoders::DefaultPlainTermEncoder;
use crate::plain_term::{PlainTermArray, PlainTermScalar};
use crate::{EncodingName, TermEncoder};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::ScalarValue;
use rdf_fusion_model::{TermRef, ThinError};
use std::clone::Clone;
use std::sync::LazyLock;
use rdf_fusion_common::DFResult;

/// Represents the fields of the [PlainTermEncoding].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlainTermEncodingField {
    /// Indicates the type of RDF term.
    TermType,
    /// Contains the lexical value of an RDF term.
    Value,
    /// Holds the data type of RDF literal, including simple literals and language-tagged literals.
    /// If an RDF term has a language tag, the datatype must contain rdf:langString.
    ///
    /// This filed should be `null` for named nodes and blank nodes.
    DataType,
    /// Contains an optional language tag for language-tagged literals.
    ///
    /// This field should be `null` for named nodes, blank nodes, and literals without a language
    /// tag.
    LanguageTag,
}

impl PlainTermEncodingField {
    pub fn name(self) -> &'static str {
        match self {
            PlainTermEncodingField::TermType => "term_type",
            PlainTermEncodingField::Value => "value",
            PlainTermEncodingField::DataType => "data_type",
            PlainTermEncodingField::LanguageTag => "language_tag",
        }
    }

    pub fn index(self) -> usize {
        match self {
            PlainTermEncodingField::TermType => 0,
            PlainTermEncodingField::Value => 1,
            PlainTermEncodingField::DataType => 2,
            PlainTermEncodingField::LanguageTag => 3,
        }
    }

    #[allow(clippy::match_same_arms)]
    pub fn data_type(self) -> DataType {
        match self {
            PlainTermEncodingField::TermType => DataType::UInt8,
            PlainTermEncodingField::Value => DataType::Utf8,
            PlainTermEncodingField::DataType => DataType::Utf8,
            PlainTermEncodingField::LanguageTag => DataType::Utf8,
        }
    }

    #[allow(clippy::match_same_arms)]
    pub fn is_nullable(self) -> bool {
        match self {
            PlainTermEncodingField::TermType => false,
            PlainTermEncodingField::Value => false,
            PlainTermEncodingField::DataType => true,
            PlainTermEncodingField::LanguageTag => true,
        }
    }

    pub fn field(self) -> Field {
        Field::new(self.name(), self.data_type(), self.is_nullable())
    }
}

static FIELDS_TYPE: LazyLock<Fields> = LazyLock::new(|| {
    let fields = vec![
        PlainTermEncodingField::TermType.field(),
        PlainTermEncodingField::Value.field(),
        PlainTermEncodingField::DataType.field(),
        PlainTermEncodingField::LanguageTag.field(),
    ];
    Fields::from(fields)
});

/// Indicates the type of an RDF term that is encoded in the [PlainTermEncoding].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PlainTermType {
    /// Represents a named node.
    NamedNode,
    /// Represents a blank node.
    BlankNode,
    /// Represents a literal.
    Literal,
}

impl TryFrom<u8> for PlainTermType {
    type Error = ThinError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(PlainTermType::NamedNode),
            1 => Ok(PlainTermType::BlankNode),
            2 => Ok(PlainTermType::Literal),
            _ => ThinError::internal_error("Unexpected type_id for encoded RDF Term"),
        }
    }
}

impl From<PlainTermType> for u8 {
    fn from(val: PlainTermType) -> Self {
        match val {
            PlainTermType::NamedNode => 0,
            PlainTermType::BlankNode => 1,
            PlainTermType::Literal => 2,
        }
    }
}

#[derive(Debug)]
pub struct PlainTermEncoding;

impl PlainTermEncoding {
    /// Returns the Arrow [Fields] of the [PlainTermEncoding].
    pub(crate) fn fields() -> Fields {
        FIELDS_TYPE.clone()
    }
}

impl TermEncoding for PlainTermEncoding {
    type Array = PlainTermArray;
    type Scalar = PlainTermScalar;

    fn name() -> EncodingName {
        EncodingName::PlainTerm
    }

    fn data_type() -> DataType {
        DataType::Struct(Self::fields().clone())
    }

    fn try_new_array(array: ArrayRef) -> DFResult<Self::Array> {
        array.try_into()
    }

    fn try_new_scalar(scalar: ScalarValue) -> DFResult<Self::Scalar> {
        scalar.try_into()
    }

    fn encode_scalar(term: TermRef<'_>) -> DFResult<Self::Scalar> {
        DefaultPlainTermEncoder::encode_term(Ok(term))
    }

    fn encode_null_scalar() -> DFResult<Self::Scalar> {
        DefaultPlainTermEncoder::encode_term(ThinError::expected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_plain_term_type_roundtrip() {
        test_roundtrip(PlainTermType::NamedNode);
        test_roundtrip(PlainTermType::BlankNode);
        test_roundtrip(PlainTermType::Literal);
    }

    fn test_roundtrip(term_field: PlainTermType) {
        let value: u8 = term_field.into();
        assert_eq!(term_field, value.try_into().unwrap());
    }
}
