use crate::typed_value::family::TypedValueFamily;

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
pub struct StringFamily {}

impl TypedValueFamily for StringFamily {
    const NAME: &'static str = "rdf-fusion.strings";
}
