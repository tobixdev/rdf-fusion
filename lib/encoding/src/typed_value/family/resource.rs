use crate::typed_value::family::TypedFamily;

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
pub struct ResourceFamily {}

impl TypedFamily for ResourceFamily {
    fn name(&self) -> &str {
        "rdf-fusion.resources"
    }
}
