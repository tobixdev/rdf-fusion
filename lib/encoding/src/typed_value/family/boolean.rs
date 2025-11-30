use crate::typed_value::family::{TypeClaim, TypeFamily};
use datafusion::arrow::datatypes::DataType;
use std::fmt::{Debug, Formatter};

/// A family that only stores Boolean values.
///
/// # Layout
///
///  Boolean Array
/// ┌───────┐
/// │ true  │
/// │───────│
/// │ false │
/// │───────│
/// │ true  │
/// └───────┘
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct BooleanFamily {
    data_type: DataType,
}

impl BooleanFamily {
    /// Creates a new [`BooleanFamily`].
    pub fn new() -> Self {
        Self {
            data_type: DataType::Boolean,
        }
    }
}

impl TypeFamily for BooleanFamily {
    fn id(&self) -> &str {
        "rdf-fusion.boolean"
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn claim(&self) -> &TypeClaim {
        todo!()
    }
}

impl Debug for BooleanFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.id())
    }
}
