use graphfusion::store::Store;
use std::time::Duration;

#[allow(unused, reason = "Not yet implemented")]
pub const MAX_SPARQL_BODY_SIZE: u64 = 1024 * 1024 * 128; // 128MB
#[allow(unused, reason = "Not yet implemented")]
pub const HTTP_TIMEOUT: Duration = Duration::from_secs(60);

/// Holds the configuration for a RdfFusion web server.
pub struct ServerConfig {
    /// The RdfFusion instance that is used.
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
