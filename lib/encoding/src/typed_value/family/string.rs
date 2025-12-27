use crate::typed_value::family::{create_struct_scalar, TypeClaim, TypeFamily};
use datafusion::arrow::array::{Array, AsArray, GenericStringArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{exec_err, ScalarValue};
use rdf_fusion_model::vocab::{rdf, xsd};
use rdf_fusion_model::{DFResult, TermRef};
use std::collections::BTreeSet;
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
    /// The claim of this family.
    claim: TypeClaim,
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
        let mut types = BTreeSet::new();
        types.insert(xsd::STRING.into());
        types.insert(rdf::LANG_STRING.into());
        Self {
            data_type: DataType::Struct(FIELDS_STRING.clone()),
            claim: TypeClaim::Literal(types),
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
        &self.claim
    }

    fn encode_value(&self, value: TermRef<'_>) -> DFResult<ScalarValue> {
        match value {
            TermRef::Literal(lit) => {
                let val = ScalarValue::Utf8(Some(lit.value().to_string()));
                let lang = if let Some(l) = lit.language() {
                    ScalarValue::Utf8(Some(l.to_string()))
                } else {
                    ScalarValue::Utf8(None)
                };
                create_struct_scalar(vec![val, lang], FIELDS_STRING.clone())
            }
            _ => exec_err!("StringFamily can only encode literals"),
        }
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
