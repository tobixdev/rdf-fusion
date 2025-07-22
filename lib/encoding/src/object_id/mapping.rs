use async_trait::async_trait;
use rdf_fusion_model::TermRef;
use std::fmt::Debug;

/// TODO
#[async_trait]
pub trait ObjectIdMapping: Debug + Send + Sync {
    /// TODO
    async fn resolve(&self, id: TermRef<'_>) -> Option<i64>;

    /// TODO
    async fn create_or_resolve(&self, id: TermRef<'_>) -> i64;
}
