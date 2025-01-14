use datafusion::logical_expr::LogicalPlan;
use spargebra::Query;

pub struct SparqlToDataFusionRewriter {}

impl SparqlToDataFusionRewriter {
    pub fn new() -> Self {
        Self {}
    }

    pub fn rewrite(&self, query: &Query) -> LogicalPlan {
        unimplemented!()
    }
}
