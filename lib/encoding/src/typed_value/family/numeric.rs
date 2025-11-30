use crate::typed_value::family::{TypeClaim, TypeFamily};
use datafusion::arrow::array::{
    Array, AsArray, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
    UnionArray,
};
use datafusion::arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
use rdf_fusion_model::Decimal;
use std::fmt::{Debug, Formatter};
use std::sync::LazyLock;

/// Family of numeric values, including `xsd:float`, `xsd:double`, `xsd:decimal`, `xsd:int` and
/// `xsd:integer`. Numeric types that are not part of this family are promoted to one of the
/// supported types.
///
/// # Layout
///
/// The layout of the numeric family is a dense union array with variants for the different
/// numeric types.
///
/// ┌────────────────────────────────────────────────────────────────┐
/// │ Union Array (Dense)                                            │
/// │                                                                │
/// │  Type Ids     Float      Double    Decimal   Int      Integer  │
/// │  ┌───────┐   ┌──────┐   ┌──────┐   ┌─────┐   ┌────┐   ┌───┐    │
/// │  │ 0     │   │ 1.2  │   │ 3.4  │   │ 5.6 │   │ 7  │   │ 8 │    │
/// │  │───────│   └──────┘   └──────┘   └─────┘   └────┘   └───┘    │
/// │  │ 1     │                                                     │
/// │  │───────│                                                     │
/// │  │ 2     │                                                     │
/// │  │───────│                                                     │
/// │  │ 3     │                                                     │
/// │  │───────│                                                     │
/// │  │ 4     │                                                     │
/// │  └───────┘                                                     │
/// │                                                                │
/// └────────────────────────────────────────────────────────────────┘
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct NumericFamily {
    /// The data type of this family.
    data_type: DataType,
}

static FIELDS_TYPE: LazyLock<UnionFields> = LazyLock::new(|| {
    let fields = vec![
        Field::new("float", DataType::Float32, false),
        Field::new("double", DataType::Float64, false),
        Field::new(
            "decimal",
            DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE),
            false,
        ),
        Field::new("int", DataType::Int32, false),
        Field::new("integer", DataType::Int64, false),
    ];

    UnionFields::new(0..fields.len() as i8, fields)
});

impl NumericFamily {
    /// Creates a new [`NumericFamily`].
    pub fn new() -> Self {
        Self {
            data_type: DataType::Union(FIELDS_TYPE.clone(), UnionMode::Dense),
        }
    }
}

impl TypeFamily for NumericFamily {
    fn id(&self) -> &str {
        "rdf-fusion.numeric"
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn claim(&self) -> &TypeClaim {
        todo!()
    }
}

impl Debug for NumericFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.id())
    }
}

/// A reference to the child arrays of a [`NumericFamily`] array.
#[derive(Debug, Clone, Copy)]
pub struct NumericArrayParts<'data> {
    /// The union array containing union of all numeric types.
    pub union_array: &'data UnionArray,
    /// The array of floats.
    pub floats: &'data Float32Array,
    /// The array of doubles.
    pub doubles: &'data Float64Array,
    /// The array of decimals.
    pub decimals: &'data Decimal128Array,
    /// The array of ints.
    pub ints: &'data Int32Array,
    /// The array of integers.
    pub integers: &'data Int64Array,
}

impl<'data> NumericArrayParts<'data> {
    /// Creates a [`NumericArrayParts`] from the given array.
    ///
    /// Panics if the array does not match the expected schema.
    pub fn from_array(array: &'data dyn Array) -> Self {
        let union_array = array.as_union();
        Self {
            union_array,
            floats: union_array.child(0).as_primitive(),
            doubles: union_array.child(1).as_primitive(),
            decimals: union_array.child(2).as_primitive(),
            ints: union_array.child(3).as_primitive(),
            integers: union_array.child(4).as_primitive(),
        }
    }
}
