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
pub(crate) enum SortableTermField {
    /// TODO
    Type,
    /// TODO
    Numeric,
    /// TODO
    Bytes,
}

impl SortableTermField {
    pub fn name(self) -> &'static str {
        match self {
            SortableTermField::Type => "type",
            SortableTermField::Numeric => "numeric",
            SortableTermField::Bytes => "bytes",
        }
    }

    pub fn index(self) -> usize {
        match self {
            SortableTermField::Type => 0,
            SortableTermField::Numeric => 1,
            SortableTermField::Bytes => 2,
        }
    }

    pub fn data_type(self) -> DataType {
        match self {
            SortableTermField::Type => DataType::UInt8,
            SortableTermField::Numeric => DataType::Float64,
            SortableTermField::Bytes => DataType::Binary,
        }
    }
}

static FIELDS: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new(
            SortableTermField::Type.name(),
            SortableTermField::Type.data_type(),
            false,
        ),
        Field::new(
            SortableTermField::Numeric.name(),
            SortableTermField::Numeric.data_type(),
            true,
        ),
        Field::new(
            SortableTermField::Bytes.name(),
            SortableTermField::Bytes.data_type(),
            false,
        ),
    ])
});

#[derive(Debug)]
pub struct SortableTermEncoding;

impl SortableTermEncoding {
    pub fn fields() -> Fields {
        FIELDS.clone()
    }

    pub fn data_type() -> DataType {
        DataType::Struct(FIELDS.clone())
    }
}

impl TermEncoding for SortableTermEncoding {
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
