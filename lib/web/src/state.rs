use rdf_fusion::store::Store;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    #[allow(unused, reason = "Not yet implemented")]
    pub read_only: bool,
    pub union_default_graph: bool,
}
