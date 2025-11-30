use crate::typed_value::family::date_time::DateTimeFamily;
use crate::typed_value::family::TypeFamily;
use datafusion::arrow::datatypes::DataType;
use std::fmt::{Debug, Formatter};

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
pub struct NumericFamily {}

impl TypeFamily for NumericFamily {
    fn id(&self) -> &str {
        "rdf-fusion.numeric"
    }

    fn data_type(&self) -> &DataType {
        todo!()
    }
}

impl Debug for NumericFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.id())
    }
}
