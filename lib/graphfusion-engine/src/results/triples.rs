use crate::sparql::error::EvaluationError;
use oxrdf::Triple;

/// An iterator over the triples that compose a graph solution.
///
/// ```
/// use graphfusion::sparql::QueryResults;
/// use graphfusion::store::Store;
///
/// let store = Store::new()?;
/// if let QueryResults::Graph(triples) = store.query("CONSTRUCT WHERE { ?s ?p ?o }")? {
///     for triple in triples {
///         println!("{}", triple?);
///     }
/// }
/// # Result::<_, Box<dyn std::error::Error>>::Ok(())
/// ```
pub struct QueryTripleStream {}

impl Iterator for QueryTripleStream {
    type Item = Result<Triple, EvaluationError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        unimplemented!()
    }
}
