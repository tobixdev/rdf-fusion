use datafusion::arrow::datatypes::{Field, Fields};
use datafusion::common::{plan_err, DFSchema, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use graphfusion_encoding::plain_term::PlainTermEncoding;
use graphfusion_encoding::TermEncoding;
use spargebra::term::{GraphNamePattern, NamedNodePattern, QuadPattern, TermPattern};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

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

/// Computes the schema of evaluating a [QuadPattern].
///
/// All RDF terms are encoded with the [PlainTermEncoding] and cannot be `null`.
fn compute_schema(pattern: &QuadPattern) -> DFSchemaRef {
    let mut variables = Vec::new();

    if let GraphNamePattern::Variable(var) = &pattern.graph_name {
        variables.push(var);
    }
    if let TermPattern::Variable(var) = &pattern.subject {
        variables.push(var);
    }
    if let NamedNodePattern::Variable(var) = &pattern.predicate {
        variables.push(var);
    }
    if let TermPattern::Variable(var) = &pattern.object {
        variables.push(var);
    }

    let fields = variables
        .iter()
        .map(|v| Field::new(v.as_str(), PlainTermEncoding::data_type(), false))
        .collect::<HashSet<_>>();
    let schema =
        DFSchema::from_unqualified_fields(fields.into_iter().collect::<Fields>(), HashMap::new())
            .expect("Variables have unique names after de-duplication.");
    Arc::new(schema)
}
