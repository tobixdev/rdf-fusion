use crate::typed_value::family::{TypeClaim, TypeFamily};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, ScalarValue};
use rdf_fusion_model::{vocab::xsd, DFResult, TermRef};
use std::collections::BTreeSet;
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
    claim: TypeClaim,
}

impl BooleanFamily {
    /// Creates a new [`BooleanFamily`].
    pub fn new() -> Self {
        let mut types = BTreeSet::new();
        types.insert(xsd::BOOLEAN.into());
        Self {
            data_type: DataType::Boolean,
            claim: TypeClaim::Literal(types),
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
        &self.claim
    }

    fn encode_value(&self, value: TermRef<'_>) -> DFResult<ScalarValue> {
        match value {
            TermRef::Literal(lit) => match lit.value() {
                "true" | "1" => Ok(ScalarValue::Boolean(Some(true))),
                "false" | "0" => Ok(ScalarValue::Boolean(Some(false))),
                _ => exec_err!("Invalid boolean value: {}", lit.value()),
            },
            _ => exec_err!("BooleanFamily can only encode literals"),
        }
    }
}

impl Debug for BooleanFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.id())
    }
}
