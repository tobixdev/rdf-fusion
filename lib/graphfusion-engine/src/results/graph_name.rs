use graphfusion_store::error::StorageError;
use oxrdf::NamedOrBlankNode;

/// An iterator returning the graph names contained in a [`Store`].
pub struct GraphNameStream {}

impl Iterator for GraphNameStream {
    type Item = Result<NamedOrBlankNode, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        unimplemented!()
    }
}
