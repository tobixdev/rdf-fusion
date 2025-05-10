mod builder;
mod term_type;

pub use builder::SortableTermBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use std::sync::LazyLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SortableTermField {
    Type,
    Numeric,
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
