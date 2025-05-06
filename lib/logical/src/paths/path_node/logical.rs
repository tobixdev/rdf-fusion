use crate::patterns::PatternNode;
use crate::DFResult;
use datafusion::common::{plan_err, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use spargebra::algebra::PropertyPathExpression;
use spargebra::term::{NamedNodePattern, TermPattern};
use std::cmp::Ordering;
use std::fmt;

#[derive(PartialEq, Eq, Hash)]
pub struct PathNode {
    graph: Option<NamedNodePattern>,
    subject: TermPattern,
    path: PropertyPathExpression,
    object: TermPattern,
    schema: DFSchemaRef,
}

impl PathNode {
    pub fn new(
        graph: Option<NamedNodePattern>,
        subject: TermPattern,
        path: PropertyPathExpression,
        object: TermPattern,
    ) -> DFResult<Self> {
        let schema = compute_schema(graph.as_ref(), &subject, &object)?;
        Ok(Self {
            graph,
            subject,
            path,
            object,
            schema,
        })
    }

    pub fn graph(&self) -> &Option<NamedNodePattern> {
        &self.graph
    }

    pub fn subject(&self) -> &TermPattern {
        &self.subject
    }

    pub fn path(&self) -> &PropertyPathExpression {
        &self.path
    }

    pub fn object(&self) -> &TermPattern {
        &self.object
    }
}

impl fmt::Debug for PathNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for PathNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for PathNode {
    fn name(&self) -> &str {
        "Path"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        Vec::new()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Path: {} {} {} {}",
            &self
                .graph
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_default(),
            self.subject,
            self.path,
            self.object
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        if !inputs.is_empty() {
            return plan_err!("Expected 0 inputs but got {}", inputs.len());
        }
        if !exprs.is_empty() {
            return plan_err!("Expected 0 expressions but got {}", exprs.len());
        }
        Self::new(
            self.graph.clone(),
            self.subject.clone(),
            self.path.clone(),
            self.object.clone(),
        )
    }
}

fn compute_schema(
    graph: Option<&NamedNodePattern>,
    subject: &TermPattern,
    object: &TermPattern,
) -> DFResult<DFSchemaRef> {
    let patterns = match graph {
        None => vec![subject.clone().into(), object.clone().into()],
        Some(graph) => vec![
            graph.clone().into(),
            subject.clone().into(),
            object.clone().into(),
        ],
    };
    PatternNode::compute_schema(&patterns)
}
