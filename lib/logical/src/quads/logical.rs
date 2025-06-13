use crate::active_graph::ActiveGraph;
use datafusion::common::{plan_err, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use rdf_fusion_encoding::typed_value::DEFAULT_QUAD_DFSCHEMA;
use rdf_fusion_model::NamedNode;
use spargebra::term::{Subject, Term};
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Formatter;

/// TODO
///
/// ### Planning QuadsNode
///
/// Planning the QuadsNode requires users to define a specialized planner for the used storage
/// layer. This is because the planner should consider storage-specific problems like sharing a
/// transaction across multiple scans of the quads table in a single query. The built-in storage
/// layers of RdfFusion provide examples.
#[derive(PartialEq, Eq, Hash)]
pub struct QuadsNode {
    active_graph: ActiveGraph,
    subject: Option<Subject>,
    predicate: Option<NamedNode>,
    object: Option<Term>,
}

impl QuadsNode {
    /// TODO
    pub fn new(
        active_graph: ActiveGraph,
        subject: Option<Subject>,
        predicate: Option<NamedNode>,
        object: Option<Term>,
    ) -> Self {
        Self {
            active_graph,
            subject,
            predicate,
            object,
        }
    }

    /// TODO
    pub fn active_graph(&self) -> &ActiveGraph {
        &self.active_graph
    }

    /// TODO
    pub fn subject(&self) -> Option<&Subject> {
        self.subject.as_ref()
    }

    /// TODO
    pub fn predicate(&self) -> Option<&NamedNode> {
        self.predicate.as_ref()
    }

    /// TODO
    pub fn object(&self) -> Option<&Term> {
        self.object.as_ref()
    }
}

impl fmt::Debug for QuadsNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
        &DEFAULT_QUAD_DFSCHEMA
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Quads")?;

        if let Some(subject) = &self.subject {
            write!(f, " subject={subject}")?;
        }

        if let Some(predicate) = &self.predicate {
            write!(f, " predicate={predicate}")?;
        }

        if let Some(object) = &self.object {
            write!(f, " object={object}")?;
        }

        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        if !inputs.is_empty() {
            return plan_err!("QuadsNode has no inputs, got {}.", inputs.len());
        }

        if !exprs.is_empty() {
            return plan_err!("QuadsNode has no expressions, got {}.", exprs.len());
        }

        Ok(Self::new(
            self.active_graph.clone(),
            self.subject.clone(),
            self.predicate.clone(),
            self.object.clone(),
        ))
    }
}
