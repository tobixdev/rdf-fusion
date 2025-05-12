use crate::encoding::TermEncoding;
use crate::plain_term::encoders::DefaultPlainTermEncoder;
use crate::plain_term::{PlainTermArray, PlainTermScalar};
use crate::sortable_term::encoders::{
    TermRefSortableTermEncoder, TypedValueRefSortableTermEncoder,
};
use crate::sortable_term::{SortableTermArray, SortableTermScalar};
use crate::{DFResult, EncodingName, TermEncoder};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::ScalarValue;
use graphfusion_model::{TermRef, ThinError};
use std::clone::Clone;
use std::sync::LazyLock;

/// TODO
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SortableTermEncodingField {
    /// TODO
    Type,
    /// TODO
    Numeric,
    /// TODO
    Bytes,
}

impl SortableTermEncodingField {
    pub fn name(self) -> &'static str {
        match self {
            SortableTermEncodingField::Type => "type",
            SortableTermEncodingField::Numeric => "numeric",
            SortableTermEncodingField::Bytes => "bytes",
        }
    }

    pub fn index(self) -> usize {
        match self {
            SortableTermEncodingField::Type => 0,
            SortableTermEncodingField::Numeric => 1,
            SortableTermEncodingField::Bytes => 2,
        }
    }

    pub fn data_type(self) -> DataType {
        match self {
            SortableTermEncodingField::Type => DataType::UInt8,
            SortableTermEncodingField::Numeric => DataType::Float64,
            SortableTermEncodingField::Bytes => DataType::Binary,
        }
    }
}

static FIELDS: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new(
            SortableTermEncodingField::Type.name(),
            SortableTermEncodingField::Type.data_type(),
            false,
        ),
        Field::new(
            SortableTermEncodingField::Numeric.name(),
            SortableTermEncodingField::Numeric.data_type(),
            true,
        ),
        Field::new(
            SortableTermEncodingField::Bytes.name(),
            SortableTermEncodingField::Bytes.data_type(),
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
    type Array = SortableTermArray;
    type Scalar = SortableTermScalar;

    fn name() -> EncodingName {
        EncodingName::Sortable
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
        TermRefSortableTermEncoder::encode_term(Ok(term))
    }

    fn encode_null_scalar() -> DFResult<Self::Scalar> {
        TermRefSortableTermEncoder::encode_term(ThinError::expected())
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
