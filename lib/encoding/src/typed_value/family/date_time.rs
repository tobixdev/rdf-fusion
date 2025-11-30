use crate::typed_value::family::TypeFamily;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use rdf_fusion_model::Decimal;
use std::fmt::{Debug, Formatter};
use std::sync::LazyLock;

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
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DateTimeFamily {
    /// The data type of the family.
    data_type: DataType,
}

/// The layout of the timestamp family.
static FIELDS_TIMESTAMP: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new("type_id", DataType::UInt8, false),
        Field::new(
            "value",
            DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE),
            false,
        ),
        Field::new("offset", DataType::Int16, true),
    ])
});

impl DateTimeFamily {
    /// Creates a new [`DateTimeFamily`].
    pub fn new() -> Self {
        Self {
            data_type: DataType::Struct(FIELDS_TIMESTAMP.clone()),
        }
    }
}

impl TypeFamily for DateTimeFamily {
    fn id(&self) -> &str {
        "rdf-fusion.date-time"
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl Debug for DateTimeFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.id())
    }
}
