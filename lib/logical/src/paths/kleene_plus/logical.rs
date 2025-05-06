use crate::paths::PATH_TABLE_DFSCHEMA;
use crate::DFResult;
use datafusion::common::{plan_err, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use std::cmp::Ordering;
use std::fmt;

/// Represents a Kleene-plus path closure node. This node computes the Kleene plus closure
/// of the inner paths. This closure is the result of the `+` operator in SPARQL property paths.
#[derive(PartialEq, Eq, Hash)]
pub struct KleenePlusClosureNode {
    /// The inner path node.
    inner: LogicalPlan,
    /// The schema of this node.
    schema: DFSchemaRef,
    /// This setting indicates whether a single path can span multiple graphs. While usually this
    /// is allowed (as the entire RDF dataset is queries), given a GRAPH ?x { ... } pattern, each
    /// named node is evaluated individually.
    allow_cross_graph_paths: bool,
}

impl KleenePlusClosureNode {
    /// Tries to create a new [KleenePlusClosureNode].
    ///
    /// See [KleenePlusClosureNode::allow_cross_graph_paths] for details on
    /// `allow_cross_graph_paths`.
    ///
    /// # Errors
    ///
    /// Returns an error if `inner` does not have the expected schema.
    pub fn try_new(inner: LogicalPlan, allow_cross_graph_paths: bool) -> DFResult<Self> {
        if inner.schema().as_ref() != PATH_TABLE_DFSCHEMA.as_ref() {
            return plan_err!(
                "Unexpected schema for inner path node. Schema: {}",
                inner.schema()
            );
        }

        Ok(Self {
            inner,
            schema: PATH_TABLE_DFSCHEMA.clone(),
            allow_cross_graph_paths,
        })
    }

    pub fn inner(&self) -> &LogicalPlan {
        &self.inner
    }

    /// Indicates whether paths can cross graphs.
    pub fn allow_cross_graph_paths(&self) -> bool {
        self.allow_cross_graph_paths
    }
}

impl fmt::Debug for KleenePlusClosureNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for KleenePlusClosureNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for KleenePlusClosureNode {
    fn name(&self) -> &str {
        "KleenePlusPath"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.inner()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KleenePlusPath:",)
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        if inputs.len() != 1 {
            return plan_err!("Expected 1 input but got {}", inputs.len());
        }
        if !exprs.is_empty() {
            return plan_err!("Expected 0 expressions but got {}", exprs.len());
        }
        Self::try_new(inputs[0].clone(), self.allow_cross_graph_paths())
    }
}
