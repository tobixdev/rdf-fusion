mod sort_builder;

pub use sort_builder::RdfTermSortBuilder;

use datafusion::arrow::datatypes::{DataType, Field, Fields};
use once_cell::unsync::Lazy;

enum RdfTermSortField {
    Type,
    Numeric,
    String,
}

impl RdfTermSortField {
    pub fn name(&self) -> &'static str {
        match self {
            RdfTermSortField::Type => "type",
            RdfTermSortField::Numeric => "numeric",
            RdfTermSortField::String => "string",
        }
    }

    pub fn index(&self) -> usize {
        match self {
            RdfTermSortField::Type => 0,
            RdfTermSortField::Numeric => 1,
            RdfTermSortField::String => 2,
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            RdfTermSortField::Type => DataType::UInt8,
            RdfTermSortField::Numeric => DataType::Int64,
            RdfTermSortField::String => DataType::Utf8,
        }
    }
}

const FIELDS_RDF_TERM_SORT: Lazy<Fields> = Lazy::new(|| {
    Fields::from(vec![
        Field::new(
            RdfTermSortField::Type.name(),
            RdfTermSortField::Type.data_type(),
            false,
        ),
        Field::new(
            RdfTermSortField::Numeric.name(),
            RdfTermSortField::Numeric.data_type(),
            true,
        ),
        Field::new(
            RdfTermSortField::String.name(),
            RdfTermSortField::String.data_type(),
            true,
        ),
    ])
});

pub struct RdfTermSort {}

impl RdfTermSort {
    pub fn data_type() -> DataType {
        DataType::Struct(FIELDS_RDF_TERM_SORT.clone())
    }
}
