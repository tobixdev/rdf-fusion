use crate::encoding::TermEncoding;
use crate::sortable_term::encoders::TermRefSortableTermEncoder;
use crate::sortable_term::{SortableTermArray, SortableTermScalar};
use crate::{DFResult, EncodingName, TermEncoder};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::ScalarValue;
use rdf_fusion_model::{TermRef, ThinError};
use std::clone::Clone;
use std::sync::LazyLock;

/// Represents a sortable term encoding field.
///
/// First, the encoding differentiates between terms of different types. For example,
///
/// This encoding is currently a work-around as user-defined orderings are not yet supported in
/// DataFusion.
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
