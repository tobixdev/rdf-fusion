use crate::typed_value::family::{TypeClaim, TypeFamily};
use datafusion::arrow::array::{Array, AsArray, GenericStringArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use std::fmt::{Debug, Formatter};
use std::sync::LazyLock;

/// Family for strings and language-tagged strings. The values of the strings are stored in the
/// same array, while those with a language tag have an additional entry in the language array.
///
/// # Layout
///
/// The layout of the resource family is stored as a struct array with two fields:
/// - `values`: the array of string values
/// - `language`: an array of optional language tags
///
/// ┌───────────────────────────┐
/// │ Struct Array              │
/// │                           │
/// │   Values       Language   │
/// │  ┌──────────┐  ┌───────┐  │
/// │  │ "wave"   │  │ NULL  │  │
/// │  │──────────│  │───────│  │
/// │  │ "bye"    │  │"en"   │  │
/// │  │──────────│  │───────│  │
/// │  │ "servus" │  │"de-at"│  │
/// │  └──────────┘  └───────┘  │
/// └───────────────────────────┘
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct StringFamily {
    /// The data type of the string family.
    data_type: DataType,
}

/// The fields of the string family.
static FIELDS_STRING: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("language", DataType::Utf8, true),
    ])
});

impl StringFamily {
    /// Creates a new [`StringFamily`].
    pub fn new() -> Self {
        Self {
            data_type: DataType::Struct(FIELDS_STRING.clone()),
        }
    }
}

impl TypeFamily for StringFamily {
    fn id(&self) -> &str {
        "rdf-fusion.strings"
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn claim(&self) -> &TypeClaim {
        todo!()
    }
}

impl Debug for StringFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.id())
    }
}

/// A reference to the child arrays of a [`StringFamily`] array.
#[derive(Debug, Clone, Copy)]
pub struct StringArrayParts<'data> {
    /// The array of string values.
    pub value: &'data GenericStringArray<i32>,
    /// The array of optional language tags.
    pub language: &'data GenericStringArray<i32>,
}

impl<'data> StringArrayParts<'data> {
    /// Creates a [`StringArrayParts`] from the given array.
    ///
    /// Panics if the array does not match the expected schema.
    pub fn from_array(array: &'data dyn Array) -> Self {
        let array = array.as_struct();
        Self {
            value: array.column(0).as_string(),
            language: array.column(1).as_string(),
        }
    }
}
