use crate::builtin::{BuiltinName, GraphFusionBuiltinFactory};
use std::collections::HashMap;
use std::sync::Arc;

/// TODO
pub type GraphFusionBuiltinRegistryRef = Arc<GraphFusionBuiltinRegistry>;

/// TODO
#[derive(Debug)]
pub struct GraphFusionBuiltinRegistry {
    /// TODO
    builtins_scalar: HashMap<BuiltinName, Arc<dyn GraphFusionBuiltinFactory>>,
}

impl Default for GraphFusionBuiltinRegistry {
    fn default() -> Self {
        let mut builtins_scalar = HashMap::new();

        todo!("Register all built-ins");

        Self { builtins_scalar }
    }
}

impl GraphFusionBuiltinRegistry {
    /// TODO
    pub fn scalar_factory(&self, name: BuiltinName) -> Arc<dyn GraphFusionBuiltinFactory> {
        let factory = self
            .builtins_scalar
            .get(&name)
            .expect("Validation checks that all built-ins are registered");
        Arc::clone(factory)
    }
}
