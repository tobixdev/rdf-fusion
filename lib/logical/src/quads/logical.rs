use crate::DFResult;
use datafusion::common::{plan_err, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use spargebra::term::GraphNamePattern;
use std::cmp::Ordering;
use std::fmt;

/// TODO
#[derive(PartialEq, Eq, Hash)]
pub struct QuadsNode {
    /// Specifies which graphs should be queried.
    graph_name: Option<GraphNamePattern>,
}

impl QuadsNode {
    /// TODO
    pub fn try_new(graph_name: Option<GraphNamePattern>) -> DFResult<Self> {
        todo!()
    }

    pub(crate) fn compute_schema(graph_name: Option<GraphNamePattern>) -> DFResult<DFSchemaRef> {
        todo!()
    }

    pub fn graph_name(&self) -> Option<&GraphNamePattern> {
        self.graph_name.as_ref()
    }
}

impl fmt::Debug for QuadsNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for QuadsNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for QuadsNode {
    fn name(&self) -> &str {
        "Quads"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        todo!()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Quads:",)?;
        todo!("Include pattern")
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        if !inputs.is_empty() {
            return plan_err!("QuadsNode has no inputs, got {}", inputs.len());
        }

        if !exprs.is_empty() {
            return plan_err!("QuadsNode must have no expressions");
        }

        todo!()
    }
}
