use crate::typed_value::family::TypedFamily;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use std::sync::LazyLock;

/// A catch-all family for literals that are not claimed by any other registered family. This
/// represents the "unknown" literal types.
///
/// # Layout
///
/// The layout of the unknown family is stored as a struct array with two fields:
/// - `values`: the array of string values
/// - `language`: the array of literal datatypes
/// ┌─────────────────────────────┐
/// │ Struct Array                │
/// │                             │
/// │   Value        Data Type    │
/// │  ┌──────────┐  ┌──────────┐ │
/// │  │ "42"     │  │"my:int"  │ │
/// │  │──────────│  │──────────│ │
/// │  │ "true"   │  │"my:bool" │ │
/// │  │──────────│  │──────────│ │
/// │  │ "1.23e4" │  │"my:float"│ │
/// │  └──────────┘  └──────────┘ │
/// └─────────────────────────────┘
pub struct UnknownFamily {
    /// The data type of the unknown family.
    data_type: DataType,
}

/// The fields of the unknown family.
static FIELDS_UNKNOWN: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("datatype", DataType::Utf8, false),
    ])
});

impl UnknownFamily {
    /// Creates a new [`UnknownFamily`].
    pub fn new() -> Self {
        Self {
            data_type: DataType::Struct(FIELDS_UNKNOWN.clone()),
        }
    }
}

impl TypedFamily for UnknownFamily {
    fn name(&self) -> &str {
        "rdf-fusion.unknown"
    }

    fn data_type(&self) -> &DataType {
        todo!()
    }
}
