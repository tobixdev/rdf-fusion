use datafusion::common::{DFSchemaRef, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

/// A logical node that represents the SPARQL `Minus` operator.
#[derive(PartialEq, Eq, Hash)]
pub struct MinusNode {
    lhs: LogicalPlan,
    rhs: LogicalPlan,
    schema: DFSchemaRef,
}

impl MinusNode {
    /// Creates a new [MinusNode].
    pub fn new(lhs: LogicalPlan, rhs: LogicalPlan) -> Self {
        let schema = Arc::clone(lhs.schema());
        Self { lhs, rhs, schema }
    }

    /// Returns the left-hand side of the node.
    pub fn lhs(&self) -> &LogicalPlan {
        &self.lhs
    }

    /// Returns the right-hand side of the node.
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
        if !exprs.is_empty() {
            return plan_err!("MinusNode must not have any expression.");
        }

        let input_len = inputs.len();
        let Ok([lhs, rhs]) = TryInto::<[LogicalPlan; 2]>::try_into(inputs) else {
            return plan_err!(
                "MinusNode must have exactly two inputs, actual: {input_len}"
            );
        };

        Ok(Self::new(lhs, rhs))
    }
}
