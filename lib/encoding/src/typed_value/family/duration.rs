use crate::typed_value::family::TypedValueFamily;

/// Family for `xsd:duration`, `xsd:yearMonthDuration` and `xsd:dayTimeDuration`.
///
/// While both arrays are nullable, functions creating values of this type must ensure that for a
/// single value, at least one of the two arrays is valid.
///
/// # Layout
///
/// The layout of the duration family is stored as a struct array with two fields:
/// - `months`: an optional array of month values
/// - `seconds`: an optional array of second values
///
/// Depending on which fields are set, the duration is interpreted as one of the three duration
/// types.
///
/// ┌─────────────────────────────┐
/// │ Struct Array                │
/// │                             │
/// │   Months        Seconds     │
/// │  ┌──────────┐  ┌──────────┐ │
/// │  │ 12       │  │ NULL     │ │
/// │  │──────────│  │──────────│ │
/// │  │ NULL     │  │ 3600     │ │
/// │  │──────────│  │──────────│ │
/// │  │ 24       │  │ 120      │ │
/// │  └──────────┘  └──────────┘ │
/// └─────────────────────────────┘
pub struct DurationFamily {}

impl TypedValueFamily for DurationFamily {
    const NAME: &'static str = "rdf-fusion.duration";
}
