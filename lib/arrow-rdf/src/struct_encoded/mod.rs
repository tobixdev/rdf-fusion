mod builder;

pub use builder::StructEncTermBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use once_cell::unsync::Lazy;

enum StructEncTermField {
    Type,
    Numeric,
    String,
}

impl StructEncTermField {
    pub fn name(&self) -> &'static str {
        match self {
            StructEncTermField::Type => "type",
            StructEncTermField::Numeric => "numeric",
            StructEncTermField::String => "string",
        }
    }

    pub fn index(&self) -> usize {
        match self {
            StructEncTermField::Type => 0,
            StructEncTermField::Numeric => 1,
            StructEncTermField::String => 2,
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            StructEncTermField::Type => DataType::UInt8,
            StructEncTermField::Numeric => DataType::Float64,
            StructEncTermField::String => DataType::Utf8,
        }
    }
}

const FIELDS_STRUCT_ENC_TERM: Lazy<Fields> = Lazy::new(|| {
    Fields::from(vec![
        Field::new(
            StructEncTermField::Type.name(),
            StructEncTermField::Type.data_type(),
            false,
        ),
        Field::new(
            StructEncTermField::Numeric.name(),
            StructEncTermField::Numeric.data_type(),
            true,
        ),
        Field::new(
            StructEncTermField::String.name(),
            StructEncTermField::String.data_type(),
            true,
        ),
    ])
});

pub struct StructEncTerm {}

impl StructEncTerm {
    pub fn fields() -> Fields {
        FIELDS_STRUCT_ENC_TERM.clone()
    }

    pub fn data_type() -> DataType {
        DataType::Struct(FIELDS_STRUCT_ENC_TERM.clone())
    }
}
