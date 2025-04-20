use crate::DFResult;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use spargebra::algebra::PropertyPathExpression;
use spargebra::term::{NamedNodePattern, TermPattern};
use std::cmp::Ordering;
use std::fmt;
use crate::patterns::PatternNode;

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
        let schema = compute_schema(graph.as_ref(), &subject, &object);
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
                .map(|g| g.to_string())
                .unwrap_or("".to_owned()),
            self.subject,
            self.path,
            self.object
        )
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        assert_eq!(inputs.len(), 0, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Ok(Self::new(
            self.graph.clone(),
            self.subject.clone(),
            self.path.clone(),
            self.object.clone(),
        )?)
    }
}

fn compute_schema(
    graph: Option<&NamedNodePattern>,
    subject: &TermPattern,
    object: &TermPattern,
) -> DFSchemaRef {
    let patterns = match graph {
        None => vec![subject.clone(), object.clone()],
        Some(graph) => vec![
            graph.clone().into_term_pattern(),
            subject.clone(),
            object.clone(),
        ],
    };
    PatternNode::compute_schema(&patterns)
}
