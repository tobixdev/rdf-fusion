use crate::typed_value::family::TypeFamily;
use datafusion::arrow::datatypes::{DataType, Field, UnionFields};
use std::fmt::{Debug, Formatter};
use std::sync::LazyLock;

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
                datafusion::arrow::datatypes::UnionMode::Dense,
            ),
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
}

impl Debug for ResourceFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.id())
    }
}
