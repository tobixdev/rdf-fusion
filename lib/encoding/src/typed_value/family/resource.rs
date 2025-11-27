use crate::typed_value::family::TypedValueFamily;

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

impl TypedValueFamily for ResourceFamily {
    const NAME: &'static str = "rdf-fusion.resources";
}
