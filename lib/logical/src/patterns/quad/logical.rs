use datafusion::common::{plan_err, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use spargebra::term::QuadPattern;
use std::cmp::Ordering;
use std::fmt;

#[derive(PartialEq, Eq, Hash)]
pub struct QuadPatternNode {
    pattern: QuadPattern,
    schema: DFSchemaRef,
}

impl QuadPatternNode {
    /// Creates a new [QuadPatternNode].
    pub fn new(pattern: QuadPattern) -> Self {
        let schema = compute_schema(&pattern);
        Self { pattern, schema }
    }

    pub fn pattern(&self) -> &QuadPattern {
        &self.pattern
    }
}

impl fmt::Debug for QuadPatternNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for QuadPatternNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for QuadPatternNode {
    fn name(&self) -> &str {
        "QuadPattern"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Pattern: {}", &self.pattern)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        if !inputs.is_empty() {
            return plan_err!(
                "PatternNode must have exactly one input, got {}",
                inputs.len()
            );
        }

        if !exprs.is_empty() {
            return plan_err!("PatternNode must have no expressions");
        }

        Ok(Self::new(self.pattern.clone()))
    }
}

pub(crate) fn compute_schema(pattern: &QuadPattern) -> DFSchemaRef {
    todo!()
}
