use crate::typed_value::family::{create_struct_scalar, TypeClaim, TypeFamily};
use datafusion::arrow::array::{Array, AsArray, GenericStringArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{exec_err, ScalarValue};
use rdf_fusion_model::{DFResult, TermRef};
use std::fmt::{Debug, Formatter};
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
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct UnknownFamily {
    /// The data type of the unknown family.
    data_type: DataType,
    /// The claim of this family.
    claim: TypeClaim,
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
            claim: TypeClaim::UnknownLiterals,
        }
    }
}

impl TypeFamily for UnknownFamily {
    fn id(&self) -> &str {
        "rdf-fusion.unknown"
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn claim(&self) -> &TypeClaim {
        &self.claim
    }

    fn encode_value(&self, value: TermRef<'_>) -> DFResult<ScalarValue> {
        match value {
            TermRef::Literal(lit) => {
                let val = ScalarValue::Utf8(Some(lit.value().to_string()));
                let datatype = ScalarValue::Utf8(Some(lit.datatype().as_str().to_string()));
                create_struct_scalar(vec![val, datatype], FIELDS_UNKNOWN.clone())
            }
            _ => exec_err!("UnknownFamily can only encode literals"),
        }
    }
}

impl Debug for UnknownFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.id())
    }
}

/// A reference to the child arrays of a [`UnknownFamily`] array.
#[derive(Debug, Clone, Copy)]
pub struct UnknownArrayParts<'data> {
    /// The array of string values.
    pub values: &'data GenericStringArray<i32>,
    /// The array of optional language tags.
    pub data_types: &'data GenericStringArray<i32>,
}

impl<'data> UnknownArrayParts<'data> {
    /// Creates a [`UnknownArrayParts`] from the given array.
    ///
    /// Panics if the array does not match the expected schema.
    pub fn from_array(array: &'data dyn Array) -> Self {
        let array = array.as_struct();
        Self {
            values: array.column(0).as_string(),
            data_types: array.column(1).as_string(),
        }
    }
}
