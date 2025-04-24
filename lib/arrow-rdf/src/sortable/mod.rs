mod builder;
mod from_sortable_term;
mod term_type;
mod with_regular_encoding;

use crate::sortable::with_regular_encoding::EncWithRegularEncoding;
pub use builder::SortableTermBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::logical_expr::ScalarUDF;
pub use from_sortable_term::FromSortableTerm;
use std::sync::LazyLock;

pub static ENC_WITH_REGULAR_ENCODING: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::from(EncWithRegularEncoding::new()));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortableTermField {
    Type,
    Numeric,
    Bytes,
    AdditionalBytes,
    EncTermType,
}

impl SortableTermField {
    pub fn name(self) -> &'static str {
        match self {
            SortableTermField::Type => "type",
            SortableTermField::Numeric => "numeric",
            SortableTermField::Bytes => "bytes",
            SortableTermField::AdditionalBytes => "additional_bytes",
            SortableTermField::EncTermType => "enc_term_type",
        }
    }

    pub fn index(self) -> usize {
        match self {
            SortableTermField::Type => 0,
            SortableTermField::Numeric => 1,
            SortableTermField::Bytes => 2,
            SortableTermField::AdditionalBytes => 3,
            SortableTermField::EncTermType => 4,
        }
    }

    pub fn data_type(self) -> DataType {
        match self {
            SortableTermField::Numeric => DataType::Float64,
            SortableTermField::Bytes | SortableTermField::AdditionalBytes => DataType::Binary,
            SortableTermField::EncTermType | SortableTermField::Type => DataType::UInt8,
        }
    }
}

static FIELDS_SORTABLE_TERM: LazyLock<Fields> = LazyLock::new(|| {
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

pub struct SortableTerm;

impl SortableTerm {
    pub fn fields() -> Fields {
        FIELDS_SORTABLE_TERM.clone()
    }

    pub fn data_type() -> DataType {
        DataType::Struct(FIELDS_SORTABLE_TERM.clone())
    }
}
