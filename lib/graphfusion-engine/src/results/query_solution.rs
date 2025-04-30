use crate::sparql::error::QueryEvaluationError;
use arrow_rdf::value_encoding::FromEncodedTerm;
use datafusion::arrow::array::{AsArray, RecordBatch, UnionArray};
use datafusion::execution::SendableRecordBatchStream;
use futures::{Stream, StreamExt};
use model::Variable;
use model::{InternalTermRef, Term};
pub use sparesults::QuerySolution;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

/// A stream over [`QuerySolution`]s.
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
    #[inline]
    pub fn variables(&self) -> &[Variable] {
        self.variables.as_ref()
    }

    fn poll_inner(
        &mut self,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Result<QuerySolution, QueryEvaluationError>>> {
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
                        let query_solution = to_query_solution(&self.variables, &batch?)?;
                        self.current = Some(query_solution);
                        self.poll_inner(ctx)
                    }
                }
            }
            // Empty
            (None, None) => Poll::Ready(None),
        }
    }
}

impl Stream for QuerySolutionStream {
    type Item = Result<QuerySolution, QueryEvaluationError>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}

fn to_query_solution(
    variables: &Arc<[Variable]>,
    batch: &RecordBatch,
) -> Result<<Vec<QuerySolution> as IntoIterator>::IntoIter, QueryEvaluationError> {
    // TODO: error handling

    let schema = batch.schema();
    let mut result = Vec::new();
    for i in 0..batch.num_rows() {
        let mut terms = Vec::new();
        for field in schema.fields() {
            let column = batch
                .column_by_name(field.name())
                .ok_or(QueryEvaluationError::InternalError(
                    "Field was not present in result.".into(),
                ))?
                .as_union();
            let term = to_term(column, i);
            terms.push(term);
        }
        result.push((Arc::clone(variables), terms).into())
    }

    Ok(result.into_iter())
}

fn to_term(objects: &UnionArray, i: usize) -> Option<Term> {
    match InternalTermRef::from_enc_array(objects, i) {
        Ok(value) => Some(value.into_decoded()),
        Err(_) => None,
    }
}

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;
    use crate::results::query_result_for_iterator;
    use crate::sparql::QueryResults;
    use model::{BlankNode, Literal, NamedNode};
    use sparesults::QueryResultsFormat;
    use std::error::Error;
    use std::io::Cursor;

    #[test]
    fn test_send_sync() {
        fn is_send_sync<T: Send + Sync>() {}
        is_send_sync::<QuerySolution>();
    }

    #[tokio::test]
    async fn test_serialization_roundtrip() -> Result<(), Box<dyn Error>> {
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

            let terms = vec![
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
                // TODO #3: Quoted Triples
                // vec![
                //     Some(
                //         Triple::new(
                //             NamedNode::new_unchecked("http://example.com/s"),
                //             NamedNode::new_unchecked("http://example.com/p"),
                //             Triple::new(
                //                 NamedNode::new_unchecked("http://example.com/os"),
                //                 NamedNode::new_unchecked("http://example.com/op"),
                //                 NamedNode::new_unchecked("http://example.com/oo"),
                //             ),
                //         )
                //         .into(),
                //     ),
                //     None,
                // ],
            ]
            .into_iter()
            .map(|ts| Ok(QuerySolution::from((Arc::clone(&variables), ts))));
            let results = vec![
                QueryResults::Boolean(true),
                QueryResults::Boolean(false),
                query_result_for_iterator(Arc::clone(&variables), terms)?,
            ];

            for ex in results {
                let mut buffer = Vec::new();
                ex.write(&mut buffer, format).await?;
                let ex2 = QueryResults::read(Cursor::new(buffer.clone()), format)?;
                let mut buffer2 = Vec::new();
                ex2.write(&mut buffer2, format).await?;
                assert_eq!(
                    str::from_utf8(&buffer).unwrap(),
                    str::from_utf8(&buffer2).unwrap()
                );
            }
        }

        Ok(())
    }
}
