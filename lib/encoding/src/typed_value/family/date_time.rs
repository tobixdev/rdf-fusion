use crate::typed_value::family::TypedValueFamily;

/// Family of `xsd:dateTime`, `xsd:date` and `xsd:time`.
///
/// # Layout
///
/// The layout of the dates and time family is a struct array with three fields:
/// - A type id which indicates which of the three types is stored (UInt8)
/// - The value of the type (Decimal128)
/// - An offset for the timezone (Int16)
///
/// ┌──────────────────────────────────────────┐
/// │ Struct Array                             │
/// │                                          │
/// │  Type Ids     Value          Offset      │
/// │  ┌───────┐   ┌──────────┐   ┌──────────┐ │
/// │  │ 0     │   │ 10.0     │   │ NULL     │ │
/// │  │───────│   │──────────│   │──────────│ │
/// │  │ 1     │   │ 20.0     │   │ -10      │ │
/// │  │───────│   │──────────│   │──────────│ │
/// │  │ 2     │   │ 30.0     │   │ +20      │ │
/// │  └───────┘   └──────────┘   └──────────┘ │
/// └──────────────────────────────────────────┘
pub struct DateTimeFamily {}

impl TypedValueFamily for DateTimeFamily {
    const NAME: &'static str = "rdf-fusion.date-time";
}
