use std::collections::HashMap;
use std::sync::Arc;
use crate::dispatcher::SparqlOpDispatcher;

enum SparqlScalarFunction {

}

pub struct GraphFusionFunctionRegistry {
    sparql_builtins: HashMap<String, Arc<dyn SparqlOpDispatcher>>
}