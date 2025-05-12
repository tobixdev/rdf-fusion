use graphfusion_model::GraphName;

/// TODO
#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub enum ActiveGraphInfo {
    /// TODO
    #[default]
    DefaultGraph,
    /// TODO
    NamedGraphs(Vec<GraphName>),
}
