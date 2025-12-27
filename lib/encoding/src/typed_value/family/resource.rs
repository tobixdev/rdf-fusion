use crate::typed_value::family::{TypeClaim, TypeFamily};
use datafusion::arrow::datatypes::{DataType, Field, UnionFields, UnionMode};
use datafusion::common::{exec_err, ScalarValue};
use rdf_fusion_model::{DFResult, TermRef};
use std::fmt::{Debug, Formatter};
use std::sync::LazyLock;
use datafusion::arrow::array::{Array, AsArray, GenericStringArray, UnionArray};

/// Family of IRIs and blank node identifiers.
///
/// # Layout
///
/// The layout of the resource family is another dense union array.
///
/// ┌───────────────────────────────────┐
/// │ Union Array (Dense)               │
/// │                                   │
/// │  Type Ids     IRIs    Blank Nodes │
/// │  ┌───────┐  ┌───────┐  ┌───────┐  │
/// │  │ 0     │  │ <v1>  │  │ _:b2  │  │
/// │  │───────│  │───────│  └───────┘  │
/// │  │ 1     │  │ <i2>  │             │
/// │  │───────│  └───────┘             │
/// │  │ 0     │                        │
/// │  └───────┘                        │
/// │                                   │
/// └───────────────────────────────────┘
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ResourceFamily {
    /// The data type of the resource family.
    data_type: DataType,
    /// The claim of this family.
    claim: TypeClaim,
}

static FIELDS_TYPE: LazyLock<UnionFields> = LazyLock::new(|| {
    let fields = vec![
        Field::new("named_node", DataType::Utf8, false),
        Field::new("blank_node", DataType::Utf8, false),
    ];
    UnionFields::new(vec![0, 1], fields)
});

impl ResourceFamily {
    /// Creates a new [`ResourceFamily`].
    pub fn new() -> Self {
        Self {
            data_type: DataType::Union(
                FIELDS_TYPE.clone(),
                UnionMode::Dense,
            ),
            claim: TypeClaim::Resources,
        }
    }
}

impl TypeFamily for ResourceFamily {
    fn id(&self) -> &str {
        "rdf-fusion.resources"
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn claim(&self) -> &TypeClaim {
        &self.claim
    }
    
    fn encode_value(&self, value: TermRef<'_>) -> DFResult<ScalarValue> {
        match value {
            TermRef::NamedNode(nn) => {
                let inner = ScalarValue::Utf8(Some(nn.as_str().to_string()));
                Ok(ScalarValue::Union(
                    Some((0, Box::new(inner))),
                    FIELDS_TYPE.clone(),
                    UnionMode::Dense,
                ))
            }
            TermRef::BlankNode(bn) => {
                let inner = ScalarValue::Utf8(Some(bn.as_str().to_string()));
                Ok(ScalarValue::Union(
                    Some((1, Box::new(inner))),
                    FIELDS_TYPE.clone(),
                    UnionMode::Dense,
                ))
            }
            _ => exec_err!("ResourceFamily can only encode named nodes and blank nodes"),
        }
    }
}

impl Debug for ResourceFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.id())
    }
}

/// A reference to the child arrays of a [`ResourceFamily`] array.
#[derive(Debug, Clone, Copy)]
pub struct ResourceArrayParts<'data> {
    /// The union array containing the type IDs and the resource values.
    pub union_array: &'data UnionArray,
    /// The array of IRIs.
    pub iris: &'data GenericStringArray<i32>,
    /// The array of blank nodes.
    pub blank_nodes: &'data GenericStringArray<i32>,
}

impl<'data> ResourceArrayParts<'data> {
    /// Creates a [`ResourceArrayParts`] from the given array.
    ///
    /// Panics if the array does not match the expected schema.
    pub fn from_array(array: &'data dyn Array) -> Self {
        let union_array = array.as_union();
        Self {
            union_array,
            iris: union_array.child(0).as_string(),
            blank_nodes: union_array.child(1).as_string(),
        }
    }
}