use crate::typed_value::family::date_time::DateTimeFamily;
use crate::typed_value::family::TypeFamily;
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
}

impl Debug for StringFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.id())
    }
}
