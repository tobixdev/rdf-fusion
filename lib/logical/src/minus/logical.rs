use crate::DFResult;
use datafusion::common::{plan_err, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(PartialEq, Eq, Hash)]
pub struct MinusNode {
    lhs: LogicalPlan,
    rhs: LogicalPlan,
    schema: DFSchemaRef,
}

impl MinusNode {
    /// TODO
    pub fn new(lhs: LogicalPlan, rhs: LogicalPlan) -> DFResult<Self> {
        let schema = Arc::clone(&lhs.schema());
        Ok(Self { lhs, rhs, schema })
    }

    pub fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    /// TODO
    pub fn lhs(&self) -> &LogicalPlan {
        &self.lhs
    }

    /// TODO
    pub fn rhs(&self) -> &LogicalPlan {
        &self.rhs
    }
}

impl fmt::Debug for MinusNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for MinusNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for MinusNode {
    fn name(&self) -> &str {
        "Minus"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.lhs(), self.rhs()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Minus:")
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        if exprs.len() != 0 {
            return plan_err!("MinusNode must not have any expression.");
        }

        let input_len = inputs.len();
        let Ok([lhs, rhs]) = TryInto::<[LogicalPlan; 2]>::try_into(inputs) else {
            return plan_err!("MinusNode must have exactly two inputs, actual: {input_len}");
        };

        Self::new(lhs, rhs)
    }
}
