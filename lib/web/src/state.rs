use rdf_fusion::store::Store;

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    #[allow(unused, reason = "Not yet implemented")]
    pub read_only: bool,
    pub union_default_graph: bool,
}
