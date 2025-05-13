use crate::DFResult;
use datafusion::common::{plan_err, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use std::cmp::Ordering;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// TODO
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum SparqlJoinType {
    /// TODO
    Inner,
    /// TODO
    Left,
}

impl Display for SparqlJoinType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SparqlJoinType::Inner => write!(f, "Inner"),
            SparqlJoinType::Left => write!(f, "Left"),
        }
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct SparqlJoinNode {
    lhs: LogicalPlan,
    rhs: LogicalPlan,
    filter: Option<Expr>,
    join_type: SparqlJoinType,
    schema: DFSchemaRef,
}

impl SparqlJoinNode {
    /// TODO
    pub fn try_new(
        lhs: LogicalPlan,
        rhs: LogicalPlan,
        filter: Option<Expr>,
        join_type: SparqlJoinType,
    ) -> DFResult<Self> {
        let schema = compute_schema(&lhs, &rhs);
        Ok(Self {
            lhs,
            rhs,
            filter,
            join_type,
            schema,
        })
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

    /// TODO
    pub fn filter(&self) -> Option<&Expr> {
        self.filter.as_ref()
    }

    /// TODO
    pub fn join_type(&self) -> SparqlJoinType {
        self.join_type
    }
}

impl fmt::Debug for SparqlJoinNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for SparqlJoinNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for SparqlJoinNode {
    fn name(&self) -> &str {
        "SparqlJoin"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.lhs(), self.rhs()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let filter = self
            .filter
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        write!(f, "SparqlJoin: {} {}", self.join_type, &filter)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        if inputs.len() != 2 {
            return plan_err!(
                "SparqlJoinNode must have exactly two inputs, got {}.",
                inputs.len()
            );
        }

        if exprs.len() > 1 {
            return plan_err!("SparqlJoinNode must not have more than one expression.");
        }

        let filter = exprs.first().cloned();
        Self::try_new(self.lhs.clone(), self.rhs.clone(), filter, self.join_type)
    }
}

/// TODO
fn compute_schema(lhs: &LogicalPlan, rhs: &LogicalPlan) -> DFSchemaRef {
    let mut new_schema = lhs.schema().as_ref().clone();
    new_schema.merge(rhs.schema());
    Arc::new(new_schema)
}
