mod builder;
mod from_sortable_term;
mod term_type;
mod with_regular_encoding;

use crate::sortable::with_regular_encoding::EncWithRegularEncoding;
pub use builder::SortableTermBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::logical_expr::ScalarUDF;
pub use from_sortable_term::FromSortableTerm;
use once_cell::unsync::Lazy;

pub const ENC_WITH_REGULAR_ENCODING: Lazy<ScalarUDF> =
    Lazy::new(|| ScalarUDF::from(EncWithRegularEncoding::new()));

enum SortableTermField {
    Type,
    Numeric,
    Bytes,
    AdditionalBytes,
    EncTermType,
}

impl SortableTermField {
    pub fn name(&self) -> &'static str {
        match self {
            SortableTermField::Type => "type",
            SortableTermField::Numeric => "numeric",
            SortableTermField::Bytes => "bytes",
            SortableTermField::AdditionalBytes => "additional_bytes",
            SortableTermField::EncTermType => "enc_term_type",
        }
    }

    pub fn index(&self) -> usize {
        match self {
            SortableTermField::Type => 0,
            SortableTermField::Numeric => 1,
            SortableTermField::Bytes => 2,
            SortableTermField::AdditionalBytes => 3,
            SortableTermField::EncTermType => 4,
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            SortableTermField::Type => DataType::UInt8,
            SortableTermField::Numeric => DataType::Float64,
            SortableTermField::Bytes => DataType::Binary,
            SortableTermField::AdditionalBytes => DataType::Binary,
            SortableTermField::EncTermType => DataType::UInt8,
        }
    }
}

const FIELDS_SORTABLE_TERM: Lazy<Fields> = Lazy::new(|| {
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
        Field::new(
            SortableTermField::AdditionalBytes.name(),
            SortableTermField::AdditionalBytes.data_type(),
            true,
        ),
        Field::new(
            SortableTermField::EncTermType.name(),
            SortableTermField::EncTermType.data_type(),
            false,
        ),
    ])
});

pub struct SortableTerm {}

impl SortableTerm {
    pub fn fields() -> Fields {
        FIELDS_SORTABLE_TERM.clone()
    }

    pub fn data_type() -> DataType {
        DataType::Struct(FIELDS_SORTABLE_TERM.clone())
    }
}
