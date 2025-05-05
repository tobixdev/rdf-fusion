use crate::encoding::TermEncoding;
use crate::plain_term_encoding::term_decoder::PlainTermDefaultDecoder;
use crate::plain_term_encoding::term_encoder::PlainTermDefaultEncoder;
use crate::plain_term_encoding::{PlainTermArray, PlainTermScalar};
use crate::DFResult;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::ScalarValue;
use model::ThinError;
use std::clone::Clone;
use std::sync::LazyLock;

static FIELDS_TYPE: LazyLock<Fields> = LazyLock::new(|| {
    let fields = vec![
        Field::new(PlainTermEncoding::COL_TERM_TYPE, DataType::UInt8, false),
        Field::new(PlainTermEncoding::COL_VALUE, DataType::Utf8, false),
        Field::new(PlainTermEncoding::COL_DATATYPE, DataType::Utf8, false),
        Field::new(PlainTermEncoding::COL_LANGUAGE, DataType::Utf8, true),
    ];
    Fields::from(fields)
});

pub struct PlainTermEncoding;

impl PlainTermEncoding {
    const COL_TERM_TYPE: &'static str = "term_type";
    const COL_VALUE: &'static str = "value";
    const COL_DATATYPE: &'static str = "datatype";
    const COL_LANGUAGE: &'static str = "language";

    pub fn fields() -> Fields {
        FIELDS_TYPE.clone()
    }
}

impl TermEncoding for PlainTermEncoding {
    type Array = PlainTermArray;
    type Scalar = PlainTermScalar;
    type DefaultEncoder = PlainTermDefaultEncoder;
    type DefaultDecoder = PlainTermDefaultDecoder;

    fn data_type() -> DataType {
        DataType::Struct(Self::fields().clone())
    }

    fn try_new_array(array: ArrayRef) -> DFResult<Self::Array> {
        array.try_into()
    }

    fn try_new_scalar(scalar: ScalarValue) -> DFResult<Self::Scalar> {
        scalar.try_into()
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
