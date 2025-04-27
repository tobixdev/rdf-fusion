use graphfusion::store::Store;

/// Holds the configuration for a GraphFusion web server.
pub struct ServerConfig {
    /// The GraphFusion instance that is used.
    pub store: Store,
    /// The IP address or DNS name that the socket binds to.
    pub bind: String,
    /// Whether the store is read-only (TODO move to Store)
    pub read_only: bool,
    /// Whether CORS is enabled.
    pub cors: bool,
    /// Whether, by default, queries match against all graphs.
    pub union_default_graph: bool,
}
