use datafusion::arrow::datatypes::DataType;
use crate::typed_value::family::TypedFamily;

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
pub struct NumericFamily {}

impl TypedFamily for NumericFamily {
    fn name(&self) -> &str {
        "rdf-fusion.numeric"
    }

    fn data_type(&self) -> &DataType {
        todo!()
    }
}
