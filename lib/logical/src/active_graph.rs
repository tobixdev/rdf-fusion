use rdf_fusion_model::NamedOrBlankNode;

/// The active graph defines which graphs can partake in the pattern matching process.
#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub enum ActiveGraph {
    /// Only the default graph forms the active graph.
    #[default]
    DefaultGraph,
    /// A set of named graphs form the active graph. This allows expressing the user-intent of
    /// queries that use the `FROM` and `FROM NAMED` clause.
    NamedGraphs(Vec<NamedOrBlankNode>),
    /// Any named graph is part of the active graph. This corresponds to `GRAPH ?x { ... }` patterns
    /// with no explicitly defined set of named graphs in the RDF data set.
    AnyNamedGraph,
}
