use rdf_fusion_model::GraphName;
use std::fmt::Display;

/// The active graph defines which graphs can partake in the pattern matching process.
#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub enum ActiveGraph {
    /// Only the default graph forms the active graph.
    #[default]
    DefaultGraph,
    /// Any graph, including the default graph, form the active graph.
    AllGraphs,
    /// A set of graphs form the active graph. This allows expressing the user-intent of
    /// queries that use the `FROM` and `FROM NAMED` clause.
    Union(Vec<GraphName>),
    /// Any named graph is part of the active graph. This corresponds to `GRAPH ?x { ... }` patterns
    /// with no explicitly defined set of named graphs in the RDF data set.
    AnyNamedGraph,
}

/// Represents the active graph as an enumerated list of individual graphs.
///
/// This resolves concepts like [ActiveGraph::AnyNamedGraph] to a list of [GraphName].
#[derive(Clone, Debug)]
pub struct EnumeratedActiveGraph(pub Vec<GraphName>);

impl EnumeratedActiveGraph {
    /// Creates a new [EnumeratedActiveGraph].
    pub fn new(graphs: Vec<GraphName>) -> Self {
        Self(graphs)
    }
}

impl Display for ActiveGraph {
    #[allow(clippy::use_debug)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActiveGraph::DefaultGraph => write!(f, "Default Graph"),
            ActiveGraph::AllGraphs => write!(f, "All Graphs"),
            ActiveGraph::Union(graphs) => write!(f, "Union of {graphs:?}"),
            ActiveGraph::AnyNamedGraph => write!(f, "Any Named Graph"),
        }
    }
}
