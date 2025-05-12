use crate::builtin::GraphFusionUdfFactory;
use crate::factory::GraphFusionUdafFactory;
use crate::registry_builder::GraphFusionFunctionRegistryBuilder;
use crate::{DFResult, FunctionName};
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
        GraphFusionFunctionRegistryBuilder::default()
            .build()
            .expect("All built-ins are registered in default builder.")
    }
}

impl GraphFusionFunctionRegistry {
    pub fn try_new(
        scalars: HashMap<FunctionName, Arc<dyn GraphFusionUdfFactory>>,
        aggregates: HashMap<FunctionName, Arc<dyn GraphFusionUdafFactory>>,
    ) -> DFResult<Self> {
        // TODO validate.
        Ok(Self {
            scalars,
            aggregates,
        })
    }

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
