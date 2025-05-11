use crate::DFResult;
use datafusion::common::{plan_err, Column, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Display, Formatter};

/// TODO
#[derive(PartialEq, Eq, Hash)]
pub struct ExtendNode {
    /// TODO
    inner: LogicalPlan,
    /// TODO
    variable: Column,
    /// TODO
    expression: Expr,
}

impl ExtendNode {
    /// TODO
    pub fn try_new(
        inner: LogicalPlan,
        variable: Column,
        expression: Expr,
    ) -> DFResult<Self> {
        todo!()
    }

    /// TODO
    pub fn schema(&self) -> DFSchemaRef {
        todo!()
    }

    /// TODO
    pub fn inner(&self) -> &LogicalPlan {
        &self.inner
    }

    pub fn variable(&self) -> &Column {
        &self.variable
    }

    /// TODO
    pub fn expression(&self) -> &Expr {
        &self.expression
    }
}

impl fmt::Debug for ExtendNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for ExtendNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for ExtendNode {
    fn name(&self) -> &str {
        "Extend"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.inner()]
    }

    fn schema(&self) -> &DFSchemaRef {
        todo!()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![self.expression.clone()]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SparqlJoin: {} {}", &self.variable, &self.expression)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        if inputs.len() != 1 {
            return plan_err!(
                "ExtendNode must have exactly one input, got {}.",
                inputs.len()
            );
        }

        if exprs.len() == 1 {
            return plan_err!("ExtendNode must exactly one expression.");
        }

        todo!()
    }
}

fn compute_schema(
    inner: LogicalPlan,
    variable: &Column,
    expression: &Expr,
) -> DFResult<DFSchemaRef> {
    todo!()
}
