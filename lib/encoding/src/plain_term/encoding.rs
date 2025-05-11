use crate::encoding::TermEncoding;
use crate::plain_term::encoders::DefaultPlainTermEncoder;
use crate::plain_term::{PlainTermArray, PlainTermScalar};
use crate::{DFResult, EncodingName, TermEncoder};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::ScalarValue;
use graphfusion_model::{TermRef, ThinError};
use std::clone::Clone;
use std::sync::LazyLock;

/// TODO
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlainTermEncodingField {
    /// TODO
    TermType,
    /// TODO
    Value,
    /// TODO
    DataType,
    /// TODO
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

    pub fn data_type(self) -> DataType {
        match self {
            PlainTermEncodingField::TermType => DataType::UInt8,
            PlainTermEncodingField::Value => DataType::Utf8,
            PlainTermEncodingField::DataType => DataType::Utf8,
            PlainTermEncodingField::LanguageTag => DataType::Utf8,
        }
    }

    pub fn is_nullable(self) -> bool {
        match self {
            PlainTermEncodingField::TermType => false,
            PlainTermEncodingField::Value => false,
            PlainTermEncodingField::DataType => true,
            PlainTermEncodingField::LanguageTag => true,
        }
    }

    pub fn field(&self) -> Field {
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

/// TODO
pub enum PlainTermType {
    /// TODO
    NamedNode,
    /// TODO
    BlankNode,
    /// TODO
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
    pub fn fields() -> Fields {
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

#[derive(Ord, PartialOrd, PartialEq, Eq, Debug, Clone, Copy)]
pub enum TermType {
    NamedNode,
    BlankNode,
    Literal,
}

impl TryFrom<i8> for TermType {
    type Error = ThinError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => TermType::NamedNode,
            1 => TermType::BlankNode,
            2 => TermType::Literal,
            _ => return ThinError::internal_error("Unexpected type_id for encoded RDF Term"),
        })
    }
}

impl TryFrom<u8> for TermType {
    type Error = ThinError;

    #[allow(
        clippy::cast_possible_wrap,
        reason = "Self::try_from will catch any overflow as EncTermField does not have that many variants"
    )]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::try_from(value as i8)
    }
}

impl From<TermType> for i8 {
    fn from(value: TermType) -> Self {
        match value {
            TermType::NamedNode => 0,
            TermType::BlankNode => 1,
            TermType::Literal => 2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_ids() {
        test_roundtrip(TermType::NamedNode);
        test_roundtrip(TermType::BlankNode);
        test_roundtrip(TermType::Literal);
    }

    fn test_roundtrip(term_field: TermType) {
        let value: i8 = term_field.into();
        assert_eq!(term_field, value.try_into().unwrap());
    }
}
