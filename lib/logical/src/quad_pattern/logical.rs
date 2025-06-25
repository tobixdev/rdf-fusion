use crate::active_graph::ActiveGraph;
use crate::patterns::compute_schema_for_triple_pattern;
use datafusion::common::{plan_err, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use rdf_fusion_common::{BlankNodeMatchingMode, DFResult};
use rdf_fusion_encoding::typed_value::DEFAULT_QUAD_DFSCHEMA;
use rdf_fusion_encoding::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_model::{NamedNodePattern, TermPattern, TriplePattern, Variable, VariableRef};
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Formatter;
use std::sync::Arc;

/// TODO
///
/// ### Planning [QuadPatternNode]
///
/// Planning the [QuadPatternNode] requires users to define a specialized planner for the used
/// storage layer. This is because the planner should consider storage-specific problems like
/// sharing a transaction across multiple scans of the quads table in a single query. The built-in
/// storage layers of RdfFusion provide examples.
#[derive(PartialEq, Eq, Hash)]
pub struct QuadPatternNode {
    /// The active graph to query.
    active_graph: ActiveGraph,
    /// Whether to project the graph to a variable.
    graph_variable: Option<Variable>,
    /// The triple pattern to match.
    pattern: TriplePattern,
    /// How to handle blank nodes in the pattern.
    blank_node_mode: BlankNodeMatchingMode,
    /// The schema of the result.
    schema: DFSchemaRef,
}

impl QuadPatternNode {
    /// Creates a new [QuadPatternNode].
    pub fn new(
        active_graph: ActiveGraph,
        graph_variable: Option<Variable>,
        pattern: TriplePattern,
    ) -> Self {
        let schema = compute_schema_for_triple_pattern(
            graph_variable.as_ref().map(|v| v.as_ref()),
            &pattern,
            BlankNodeMatchingMode::Variable,
        );
        Self {
            active_graph,
            graph_variable,
            blank_node_mode: BlankNodeMatchingMode::Variable,
            pattern,
            schema,
        }
    }

    /// Creates a new [QuadPatternNode].
    ///
    /// Contrary to [Self::new], blank nodes are not treated as a variable. They are used for
    /// filtering the quad set.
    pub fn new_with_blank_nodes_as_filter(
        active_graph: ActiveGraph,
        graph_variable: Option<Variable>,
        pattern: TriplePattern,
    ) -> Self {
        let schema = compute_schema_for_triple_pattern(
            graph_variable.as_ref().map(|v| v.as_ref()),
            &pattern,
            BlankNodeMatchingMode::Filter,
        );
        Self {
            active_graph,
            graph_variable,
            blank_node_mode: BlankNodeMatchingMode::Filter,
            pattern,
            schema,
        }
    }

    /// Creates a new [QuadPatternNode] that returns all quads in `active_graph` using the default
    /// quads schema.
    pub fn new_all_quads(active_graph: ActiveGraph) -> Self {
        Self {
            active_graph,
            graph_variable: Some(Variable::new_unchecked(COL_GRAPH)),
            pattern: TriplePattern {
                subject: TermPattern::Variable(Variable::new_unchecked(COL_SUBJECT)),
                predicate: NamedNodePattern::Variable(Variable::new_unchecked(COL_PREDICATE)),
                object: TermPattern::Variable(Variable::new_unchecked(COL_OBJECT)),
            },
            blank_node_mode: BlankNodeMatchingMode::Filter, // Doesn't matter here
            schema: Arc::clone(&DEFAULT_QUAD_DFSCHEMA),
        }
    }

    /// The active graph to query.
    pub fn active_graph(&self) -> &ActiveGraph {
        &self.active_graph
    }

    /// The result mode of the [QuadPatternNode].
    pub fn graph_variable(&self) -> Option<VariableRef<'_>> {
        self.graph_variable.as_ref().map(|v| v.as_ref())
    }

    /// The triple pattern to match.
    pub fn pattern(&self) -> &TriplePattern {
        &self.pattern
    }

    /// The blank node matching mode.
    pub fn blank_node_mode(&self) -> BlankNodeMatchingMode {
        self.blank_node_mode
    }
}

impl fmt::Debug for QuadPatternNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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

    fn fmt_for_explain(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "QuadPattern (")?;

        if let Some(graph_variable) = &self.graph_variable {
            write!(f, "{graph_variable} ")?;
        }
        write!(f, "{})", &self.pattern)?;

        if self.active_graph != ActiveGraph::DefaultGraph {
            write!(f, "active_graph: {} ", self.active_graph)?;
        }

        Ok(())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DFResult<Self> {
        if !inputs.is_empty() {
            return plan_err!("QuadPatternNode has no inputs, got {}.", inputs.len());
        }

        if !exprs.is_empty() {
            return plan_err!("QuadPatternNode has no expressions, got {}.", exprs.len());
        }

        let cloned = match self.blank_node_mode {
            BlankNodeMatchingMode::Variable => Self::new(
                self.active_graph.clone(),
                self.graph_variable.clone(),
                self.pattern.clone(),
            ),
            BlankNodeMatchingMode::Filter => Self::new_with_blank_nodes_as_filter(
                self.active_graph.clone(),
                self.graph_variable.clone(),
                self.pattern.clone(),
            ),
        };
        Ok(cloned)
    }
}
