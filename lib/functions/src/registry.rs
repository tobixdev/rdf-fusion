use crate::builtin::factory::GraphFusionUdafFactory;
use crate::builtin::GraphFusionUdfFactory;
use crate::FunctionName;
use std::collections::HashMap;
use std::sync::Arc;

/// TODO
pub type GraphFusionFunctionRegistryRef = Arc<GraphFusionFunctionRegistry>;

/// TODO
#[derive(Debug)]
pub struct GraphFusionFunctionRegistry {
    /// TODO
    scalars: HashMap<FunctionName, Arc<dyn GraphFusionUdfFactory>>,
    /// TODO
    aggregates: HashMap<FunctionName, Arc<dyn GraphFusionUdafFactory>>,
}

impl Default for GraphFusionFunctionRegistry {
    fn default() -> Self {
        let scalars = HashMap::new();
        let aggregates = HashMap::new();

        todo!("Register all built-ins");

        Self {
            scalars,
            aggregates,
        }
    }
}

impl GraphFusionFunctionRegistry {
    /// TODO
    pub fn udf_factory(&self, name: FunctionName) -> Arc<dyn GraphFusionUdfFactory> {
        let factory = self
            .scalars
            .get(&name)
            .expect("Validation checks that all built-ins are registered");
        Arc::clone(factory)
    }

    /// TODO
    pub fn udaf_factory(&self, name: FunctionName) -> Arc<dyn GraphFusionUdafFactory> {
        let factory = self
            .aggregates
            .get(&name)
            .expect("Validation checks that all built-ins are registered");
        Arc::clone(factory)
    }
}
