use crate::builtin::{BuiltinName, GraphFusionBuiltinFactory};
use std::collections::HashMap;
use std::sync::Arc;

/// TODO
pub struct GraphFusionFunctionRegistry {
    /// TODO
    builtins_scalar: HashMap<BuiltinName, Arc<dyn GraphFusionBuiltinFactory>>,
}
