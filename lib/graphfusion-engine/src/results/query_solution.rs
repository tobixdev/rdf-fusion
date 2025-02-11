use crate::sparql::error::EvaluationError;
use arrow_rdf::decoded::model::DecTermField;
use datafusion::arrow::array::{Array, AsArray, RecordBatch, UnionArray};
use datafusion::execution::SendableRecordBatchStream;
use futures::{Stream, StreamExt};
use oxrdf::{BlankNode, Literal, NamedNode, Term, Variable};
pub use sparesults::QuerySolution;
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
                        self.current = Some(to_query_solution(self.variables.clone(), &batch?)?);
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
    type Item = Result<QuerySolution, EvaluationError>;

    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(ctx)
    }
}

fn to_query_solution(
    variables: Arc<[Variable]>,
    batch: &RecordBatch,
) -> Result<<Vec<QuerySolution> as IntoIterator>::IntoIter, EvaluationError> {
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
            let term_field = DecTermField::try_from(column.type_id(i))?;
            let term = match column.is_null(i) {
                true => None,
                false => Some(to_term(column, column.value_offset(i), term_field)?),
            };
            terms.push(term);
        }
        result.push((variables.clone(), terms).into())
    }

    Ok(result.into_iter())
}

fn to_term(
    objects: &UnionArray,
    i: usize,
    term_field: DecTermField,
) -> Result<Term, EvaluationError> {
    Ok(match term_field {
        DecTermField::NamedNode => {
            let value = objects
                .child(term_field.type_id())
                .as_string::<i32>()
                .value(i);
            if value == "DEFAULT" {
                Term::Literal(Literal::new_simple_literal(value))
            } else {
                Term::NamedNode(NamedNode::new(value).map_err(EvaluationError::unexpected)?)
            }
        }
        DecTermField::BlankNode => {
            let value = objects
                .child(term_field.type_id())
                .as_string::<i32>()
                .value(i);
            Term::BlankNode(
                BlankNode::new(value).map_err(|err| EvaluationError::Unexpected(Box::new(err)))?,
            )
        }
        DecTermField::String => {
            let values = objects
                .child(term_field.type_id())
                .as_struct()
                .column_by_name("value")
                .expect("Schema is fixed")
                .as_string::<i32>();
            let language = objects
                .child(term_field.type_id())
                .as_struct()
                .column_by_name("language")
                .expect("Schema is fixed")
                .as_string::<i32>();

            if language.is_null(i) {
                Term::Literal(Literal::new_simple_literal(String::from(values.value(i))))
            } else {
                Term::Literal(
                    Literal::new_language_tagged_literal(
                        String::from(values.value(i)),
                        String::from(language.value(i)),
                    )
                    .map_err(EvaluationError::unexpected)?,
                )
            }
        }
        DecTermField::TypedLiteral => {
            let values = objects
                .child(term_field.type_id())
                .as_struct()
                .column_by_name("value")
                .expect("Schema is fixed")
                .as_string::<i32>();
            let datatypes = objects
                .child(term_field.type_id())
                .as_struct()
                .column_by_name("datatype")
                .expect("Schema is fixed")
                .as_string::<i32>();
            Term::Literal(Literal::new_typed_literal(
                String::from(values.value(i)),
                NamedNode::new(String::from(datatypes.value(i)))
                    .map_err(EvaluationError::unexpected)?,
            ))
        }
    })
}

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;
    use crate::results::query_result_for_iterator;
    use crate::sparql::QueryResults;
    use oxrdf::{BlankNode, Literal, NamedNode, Triple};
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
            ].into_iter().map(|ts| Ok(QuerySolution::from((variables.clone(), ts))));
            let results = vec![
                QueryResults::Boolean(true),
                QueryResults::Boolean(false),
                query_result_for_iterator(variables.clone(), terms)?,
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
