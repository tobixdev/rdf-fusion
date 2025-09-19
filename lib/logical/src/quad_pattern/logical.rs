use crate::active_graph::ActiveGraph;
use crate::patterns::compute_schema_for_triple_pattern;
use datafusion::common::{DFSchemaRef, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use rdf_fusion_model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_model::{BlankNodeMatchingMode, DFResult};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_model::{
    NamedNodePattern, TermPattern, TriplePattern, Variable, VariableRef,
};
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Formatter;

/// A logical node that represents a scan of quads matching a pattern.
///
/// This node is the main entry point for accessing RDF data in the query plan.
/// It is responsible for retrieving quads from the underlying storage that match
/// the given `active_graph` and `pattern`.
///
/// ### Blank Node Matching
///
/// The `blank_node_mode` determines how blank nodes in the pattern are handled.
/// See [BlankNodeMatchingMode] for more details.
///
/// ### Planning [QuadPatternNode]
///
/// Planning the [QuadPatternNode] requires users to define a specialized planner for the used
/// storage layer. This is because the planner should consider storage-specific problems like
/// sharing a transaction across multiple scans of the quads table in a single query. The built-in
/// storage layers of RDF Fusion provide examples.
#[derive(PartialEq, Eq, Hash)]
pub struct QuadPatternNode {
    /// The encoding of the storage layer.
    storage_encoding: QuadStorageEncoding,
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
        storage_encoding: QuadStorageEncoding,
        active_graph: ActiveGraph,
        graph_variable: Option<Variable>,
        pattern: TriplePattern,
    ) -> Self {
        let schema = compute_schema_for_triple_pattern(
            &storage_encoding,
            graph_variable.as_ref().map(|v| v.as_ref()),
            &pattern,
            BlankNodeMatchingMode::Variable,
        );
        Self {
            storage_encoding,
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
        storage_encoding: QuadStorageEncoding,
        active_graph: ActiveGraph,
        graph_variable: Option<Variable>,
        pattern: TriplePattern,
    ) -> Self {
        let schema = compute_schema_for_triple_pattern(
            &storage_encoding,
            graph_variable.as_ref().map(|v| v.as_ref()),
            &pattern,
            BlankNodeMatchingMode::Filter,
        );
        Self {
            storage_encoding,
            active_graph,
            graph_variable,
            blank_node_mode: BlankNodeMatchingMode::Filter,
            pattern,
            schema,
        }
    }

    /// Creates a new [QuadPatternNode] that returns all quads in `active_graph` using the default
    /// quads schema.
    pub fn new_all_quads(
        storage_encoding: QuadStorageEncoding,
        active_graph: ActiveGraph,
    ) -> Self {
        Self {
            active_graph,
            graph_variable: Some(Variable::new_unchecked(COL_GRAPH)),
            pattern: TriplePattern {
                subject: TermPattern::Variable(Variable::new_unchecked(COL_SUBJECT)),
                predicate: NamedNodePattern::Variable(Variable::new_unchecked(
                    COL_PREDICATE,
                )),
                object: TermPattern::Variable(Variable::new_unchecked(COL_OBJECT)),
            },
            blank_node_mode: BlankNodeMatchingMode::Filter, // Doesn't matter here
            schema: storage_encoding.quad_schema(),
            storage_encoding,
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
            write!(f, ", active_graph: {} ", self.active_graph)?;
        }

        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        if !inputs.is_empty() {
            return plan_err!("QuadPatternNode has no inputs, got {}.", inputs.len());
        }

        if !exprs.is_empty() {
            return plan_err!("QuadPatternNode has no expressions, got {}.", exprs.len());
        }

        let cloned = match self.blank_node_mode {
            BlankNodeMatchingMode::Variable => Self::new(
                self.storage_encoding.clone(),
                self.active_graph.clone(),
                self.graph_variable.clone(),
                self.pattern.clone(),
            ),
            BlankNodeMatchingMode::Filter => Self::new_with_blank_nodes_as_filter(
                self.storage_encoding.clone(),
                self.active_graph.clone(),
                self.graph_variable.clone(),
                self.pattern.clone(),
            ),
        };
        Ok(cloned)
    }
}
