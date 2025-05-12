use graphfusion_model::NamedOrBlankNode;

/// TODO
#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub enum ActiveGraphInfo {
    /// TODO
    #[default]
    DefaultGraph,
    /// TODO
    NamedGraphs(Vec<NamedOrBlankNode>),
}
