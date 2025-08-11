use rdf_fusion_model::QuadRef;

/// Provides the initial state for writing operation on the log.
///
/// Depending on the log retention policy, the entire store state cannot be contructed from the log.
/// However, knowing this state is crucial for many update operations. For example, inserting a quad
/// must ensure that the quad is not already present in the store. The purpose of a [LogStateRoot]
/// is to allow accessing such a state that is no longer present in the log.
pub trait LogStateRoot {
    /// Filters all existing quads from the given iterator.
    fn filter_existing_quads(
        &self,
        quads: impl IntoIterator<Item = QuadRef<'_>>,
    ) -> impl Iterator<Item = QuadRef<'_>>;

    /// Filters all non-existing quads from the given iterator.
    fn filter_non_existing_quads(
        &self,
        quads: impl IntoIterator<Item = QuadRef<'_>>,
    ) -> impl Iterator<Item = QuadRef<'_>>;
}

struct NoLogStateRoot {}

impl LogStateRoot for NoLogStateRoot {
    fn filter_existing_quads(
        &self,
        quads: impl IntoIterator<Item = QuadRef<'_>>,
    ) -> impl Iterator<Item = QuadRef<'_>> {
        quads.into_iter()
    }

    fn filter_non_existing_quads(
        &self,
        quads: impl IntoIterator<Item = QuadRef<'_>>,
    ) -> impl Iterator<Item = QuadRef<'_>> {
        quads.into_iter()
    }
}
