use crate::error::StorageError;
use crate::sparql::error::EvaluationError;
use arrow_rdf::decoded::model::{
    DEC_TYPE_ID_BLANK_NODE, DEC_TYPE_ID_NAMED_NODE, DEC_TYPE_ID_STRING, DEC_TYPE_ID_TYPED_LITERAL,
};
use datafusion::arrow::array::{Array, AsArray, RecordBatch, UnionArray};
use datafusion::execution::SendableRecordBatchStream;
use futures::{Stream, StreamExt};
use oxrdf::{BlankNode, Literal, NamedNode, Term, Variable};
pub use sparesults::QuerySolution;
use sparesults::ReaderSolutionsParser;
use std::io::Read;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

/// An iterator over [`QuerySolution`]s.
///
/// ```
/// use graphfusion::sparql::QueryResults;
/// use graphfusion::store::Store;
///
/// let store = Store::new()?;
/// if let QueryResults::Solutions(solutions) = store.query("SELECT ?s WHERE { ?s ?p ?o }")? {
///     for solution in solutions {
///         println!("{:?}", solution?.get("s"));
///     }
/// }
/// # Result::<_, Box<dyn std::error::Error>>::Ok(())
/// ```
pub struct QuerySolutionStream {
    variables: Arc<[Variable]>,
    inner: Option<SendableRecordBatchStream>,
    current: Option<<Vec<QuerySolution> as IntoIterator>::IntoIter>,
}

impl QuerySolutionStream {
    /// Construct a new iterator of solution from an ordered list of solution variables and an iterator of solution tuples
    /// (each tuple using the same ordering as the variable list such that tuple element 0 is the value for the variable 0...)
    pub fn new(variables: Arc<[Variable]>, inner: SendableRecordBatchStream) -> Self {
        Self {
            variables,
            inner: Some(inner),
            current: None,
        }
    }

    /// The variables used in the solutions.
    ///
    /// ```
    /// use graphfusion::sparql::{QueryResults, Variable};
    /// use graphfusion::store::Store;
    ///
    /// let store = Store::new()?;
    /// if let QueryResults::Solutions(solutions) = store.query("SELECT ?s ?o WHERE { ?s ?p ?o }")? {
    ///     assert_eq!(
    ///         solutions.variables(),
    ///         &[Variable::new("s")?, Variable::new("o")?]
    ///     );
    /// }
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    #[inline]
    pub fn variables(&self) -> &[Variable] {
        self.variables.as_ref()
    }

    fn poll_inner(
        &mut self,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Result<QuerySolution, EvaluationError>>> {
        match (&mut self.inner, &mut self.current) {
            // Still entries from the current batch to return
            (_, Some(iter)) => {
                let next = iter.next();
                match next {
                    None => {
                        self.current = None;
                        self.poll_inner(ctx)
                    }
                    Some(solution) => Poll::Ready(Some(Ok(solution))),
                }
            }
            // Load new batch
            (Some(stream), None) => {
                let next_batch = ready!(stream.poll_next_unpin(ctx));
                match next_batch {
                    None => {
                        self.inner = None;
                        self.poll_inner(ctx)
                    }
                    Some(batch) => {
                        // TODO: error handling
                        self.current = Some(
                            to_query_solution(self.variables.clone(), &batch.unwrap()).unwrap(),
                        );
                        self.poll_inner(ctx)
                    }
                }
            }
            // Empty
            (None, None) => Poll::Ready(None),
        }
    }
}

impl<R: Read + 'static> From<ReaderSolutionsParser<R>> for QuerySolutionStream {
    fn from(_reader: ReaderSolutionsParser<R>) -> Self {
        unimplemented!()
    }
}

impl Stream for QuerySolutionStream {
    type Item = Result<QuerySolution, EvaluationError>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(ctx)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        unimplemented!()
    }
}

fn to_query_solution(
    variables: Arc<[Variable]>,
    batch: &RecordBatch,
) -> Result<<Vec<QuerySolution> as IntoIterator>::IntoIter, StorageError> {
    // TODO: error handling

    let schema = batch.schema();
    let mut result = Vec::new();
    for i in 0..batch.num_rows() {
        let mut terms = Vec::new();
        for field in schema.fields().iter() {
            let column = batch
                .column_by_name(field.name())
                .expect("Schema must match")
                .as_union();
            let term = match column.is_null(i) {
                true => None,
                false => Some(to_term(column, column.value_offset(i), column.type_id(i))?),
            };
            terms.push(term);
        }
        result.push((variables.clone(), terms).into())
    }

    Ok(result.into_iter())
}

fn to_term(objects: &UnionArray, i: usize, type_id: i8) -> Result<Term, StorageError> {
    Ok(match type_id {
        DEC_TYPE_ID_NAMED_NODE => {
            let value = objects.child(type_id).as_string::<i32>().value(i);
            if value == "DEFAULT" {
                Term::Literal(Literal::new_simple_literal(value))
            } else {
                Term::NamedNode(NamedNode::new(value).unwrap())
            }
        }
        DEC_TYPE_ID_BLANK_NODE => {
            let value = objects.child(type_id).as_string::<i32>().value(i);
            Term::BlankNode(BlankNode::new(value).unwrap())
        }
        DEC_TYPE_ID_STRING => {
            let values = objects
                .child(type_id)
                .as_struct()
                .column_by_name("value")
                .unwrap()
                .as_string::<i32>();
            let language = objects
                .child(type_id)
                .as_struct()
                .column_by_name("language")
                .unwrap()
                .as_string::<i32>();

            if language.is_null(i) {
                // TODO: error handling?
                Term::Literal(Literal::new_simple_literal(String::from(values.value(i))))
            } else {
                // TODO: error handling?
                Term::Literal(
                    Literal::new_language_tagged_literal(
                        String::from(values.value(i)),
                        String::from(language.value(i)),
                    )
                    .unwrap(),
                )
            }
        }
        DEC_TYPE_ID_TYPED_LITERAL => {
            let values = objects
                .child(type_id)
                .as_struct()
                .column_by_name("value")
                .unwrap()
                .as_string::<i32>();
            let datatypes = objects
                .child(type_id)
                .as_struct()
                .column_by_name("datatype")
                .unwrap()
                .as_string::<i32>();
            Term::Literal(Literal::new_typed_literal(
                String::from(values.value(i)),
                NamedNode::new(String::from(datatypes.value(i))).unwrap(), // TODO: error handling?
            ))
        }
        _ => {
            unimplemented!("Proper error handling")
        }
    })
}

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;
    use crate::sparql::QueryResults;
    use oxrdf::{BlankNode, Literal, NamedNode, Term};
    use sparesults::QueryResultsFormat;
    use std::io::Cursor;

    #[test]
    fn test_send_sync() {
        fn is_send_sync<T: Send + Sync>() {}
        is_send_sync::<QuerySolution>();
    }

    #[test]
    fn test_serialization_roundtrip() -> Result<(), EvaluationError> {
        use std::str;

        for format in [
            QueryResultsFormat::Json,
            QueryResultsFormat::Xml,
            QueryResultsFormat::Tsv,
        ] {
            let variables: Arc<[Variable]> = Arc::new([
                Variable::new_unchecked("foo"),
                Variable::new_unchecked("bar"),
            ]);

            let terms: Vec<Vec<Option<Term>>> = vec![
                vec![None, None],
                vec![
                    Some(NamedNode::new_unchecked("http://example.com").into()),
                    None,
                ],
                vec![
                    None,
                    Some(NamedNode::new_unchecked("http://example.com").into()),
                ],
                vec![
                    Some(BlankNode::new_unchecked("foo").into()),
                    Some(BlankNode::new_unchecked("bar").into()),
                ],
                vec![Some(Literal::new_simple_literal("foo").into()), None],
                vec![
                    Some(Literal::new_language_tagged_literal_unchecked("foo", "fr").into()),
                    None,
                ],
                vec![
                    Some(Literal::from(1).into()),
                    Some(Literal::from(true).into()),
                ],
                vec![
                    Some(Literal::from(1.33).into()),
                    Some(Literal::from(false).into()),
                ],
                vec![
                    Some(
                        Triple::new(
                            NamedNode::new_unchecked("http://example.com/s"),
                            NamedNode::new_unchecked("http://example.com/p"),
                            Triple::new(
                                NamedNode::new_unchecked("http://example.com/os"),
                                NamedNode::new_unchecked("http://example.com/op"),
                                NamedNode::new_unchecked("http://example.com/oo"),
                            ),
                        )
                        .into(),
                    ),
                    None,
                ],
            ];
            let solutions = terms
                .into_iter()
                .map(|terms| Ok((variables.clone(), terms).into()))
                .collect::<Vec<Result<QuerySolution, EvaluationError>>>();

            let results = vec![
                QueryResults::Boolean(true),
                QueryResults::Boolean(false),
                QueryResults::Solutions(QuerySolutionStream::new(
                    variables.clone(),
                    solutions.into_iter(),
                )),
            ];

            for ex in results {
                let mut buffer = Vec::new();
                ex.write(&mut buffer, format)?;
                let ex2 = QueryResults::read(Cursor::new(buffer.clone()), format)?;
                let mut buffer2 = Vec::new();
                ex2.write(&mut buffer2, format)?;
                assert_eq!(
                    str::from_utf8(&buffer).unwrap(),
                    str::from_utf8(&buffer2).unwrap()
                );
            }
        }

        Ok(())
    }
}
