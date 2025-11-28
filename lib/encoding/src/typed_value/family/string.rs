use std::sync::LazyLock;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use crate::typed_value::family::TypedFamily;

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

impl TypedFamily for StringFamily {
    fn name(&self) -> &str {
        "rdf-fusion.strings"
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}
