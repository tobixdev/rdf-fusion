use crate::typed_value::family::{TypeClaim, TypeFamily};
use datafusion::arrow::array::{
    Array, AsArray, Decimal128Array, Int16Array, StructArray, UInt8Array,
};
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
/// │  DT Type      Value          Offset      │
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
        Field::new("date_time_type", DataType::UInt8, false),
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

    fn claim(&self) -> &TypeClaim {
        todo!()
    }
}

impl Debug for DateTimeFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.id())
    }
}

/// A reference to the child arrays of a [`DateTimeFamily`] array.
#[derive(Debug, Clone, Copy)]
pub struct DateTimeArrayParts<'data> {
    /// The struct array containing the children.
    pub struct_array: &'data StructArray,
    /// The array of months.
    pub date_time_type: &'data UInt8Array,
    /// The timestamp values array.
    pub timestamp_values: &'data Decimal128Array,
    /// The timestamp offsets array.
    pub timestamp_offsets: &'data Int16Array,
}

impl<'data> DateTimeArrayParts<'data> {
    /// Creates a [`DateTimeArrayParts`] from the given array.
    ///
    /// Panics if the array does not match the expected schema.
    pub fn from_array(array: &'data dyn Array) -> Self {
        let struct_array = array.as_struct();
        Self {
            struct_array,
            date_time_type: struct_array.column(0).as_primitive(),
            timestamp_values: struct_array.column(1).as_primitive(),
            timestamp_offsets: struct_array.column(2).as_primitive(),
        }
    }
}
