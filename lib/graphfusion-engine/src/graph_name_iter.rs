use graphfusion_store::error::StorageError;
use oxrdf::NamedOrBlankNode;

/// An iterator returning the graph names contained in a [`Store`].
pub struct GraphNameIter {}

impl Iterator for GraphNameIter {
    type Item = Result<NamedOrBlankNode, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        unimplemented!()
    }
}
