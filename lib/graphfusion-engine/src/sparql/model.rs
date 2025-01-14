use crate::sparql::error::{EvaluationError, SparqlEvaluationError};
use oxrdf::{Triple, Variable, VariableRef};
use oxrdfio::{RdfFormat, RdfSerializer};
pub use sparesults::QuerySolution;
use sparesults::ReaderSolutionsParser;
use sparesults::{
    QueryResultsFormat, QueryResultsParseError, QueryResultsParser, QueryResultsSerializer,
    ReaderQueryResultsParserOutput,
};
use std::io::{Read, Write};
use std::sync::Arc;

/// Results of a [SPARQL query](https://www.w3.org/TR/sparql11-query/).
pub enum QueryResults {
    /// Results of a [SELECT](https://www.w3.org/TR/sparql11-query/#select) query.
    Solutions(QuerySolutionIter),
    /// Result of a [ASK](https://www.w3.org/TR/sparql11-query/#ask) query.
    Boolean(bool),
    /// Results of a [CONSTRUCT](https://www.w3.org/TR/sparql11-query/#construct) or [DESCRIBE](https://www.w3.org/TR/sparql11-query/#describe) query.
    Graph(QueryTripleIter),
}

impl QueryResults {
    /// Reads a SPARQL query results serialization.
    pub fn read(
        reader: impl Read + 'static,
        format: QueryResultsFormat,
    ) -> Result<Self, QueryResultsParseError> {
        Ok(QueryResultsParser::from_format(format)
            .for_reader(reader)?
            .into())
    }

    /// Writes the query results (solutions or boolean).
    ///
    /// This method fails if it is called on the `Graph` results.
    ///
    /// ```
    /// use graphfusion::store::Store;
    /// use graphfusion::model::*;
    /// use graphfusion::sparql::results::QueryResultsFormat;
    ///
    /// let store = Store::new()?;
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// store.insert(QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph))?;
    ///
    /// let results = store.query("SELECT ?s WHERE { ?s ?p ?o }")?;
    /// assert_eq!(
    ///     results.write(Vec::new(), QueryResultsFormat::Json)?,
    ///     r#"{"head":{"vars":["s"]},"results":{"bindings":[{"s":{"type":"uri","value":"http://example.com"}}]}}"#.as_bytes()
    /// );
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub fn write<W: Write>(
        self,
        writer: W,
        format: QueryResultsFormat,
    ) -> Result<W, EvaluationError> {
        let serializer = QueryResultsSerializer::from_format(format);
        match self {
            Self::Boolean(value) => serializer.serialize_boolean_to_writer(writer, value),
            Self::Solutions(solutions) => {
                let mut serializer = serializer
                    .serialize_solutions_to_writer(writer, solutions.variables().to_vec())
                    .map_err(EvaluationError::ResultsSerialization)?;
                for solution in solutions {
                    serializer
                        .serialize(&solution?)
                        .map_err(EvaluationError::ResultsSerialization)?;
                }
                serializer.finish()
            }
            Self::Graph(triples) => {
                let s = VariableRef::new_unchecked("subject");
                let p = VariableRef::new_unchecked("predicate");
                let o = VariableRef::new_unchecked("object");
                let mut serializer = serializer
                    .serialize_solutions_to_writer(
                        writer,
                        vec![s.into_owned(), p.into_owned(), o.into_owned()],
                    )
                    .map_err(EvaluationError::ResultsSerialization)?;
                for triple in triples {
                    let triple = triple?;
                    serializer
                        .serialize([
                            (s, &triple.subject.into()),
                            (p, &triple.predicate.into()),
                            (o, &triple.object),
                        ])
                        .map_err(EvaluationError::ResultsSerialization)?;
                }
                serializer.finish()
            }
        }
        .map_err(EvaluationError::ResultsSerialization)
    }

    /// Writes the graph query results.
    ///
    /// This method fails if it is called on the `Solution` or `Boolean` results.
    ///
    /// ```
    /// use graphfusion::io::RdfFormat;
    /// use graphfusion::model::*;
    /// use graphfusion::store::Store;
    ///
    /// let graph = "<http://example.com> <http://example.com> <http://example.com> .\n";
    ///
    /// let store = Store::new()?;
    /// store.load_graph(
    ///     graph.as_bytes(),
    ///     RdfFormat::NTriples,
    ///     GraphName::DefaultGraph,
    ///     None,
    /// )?;
    ///
    /// let results = store.query("CONSTRUCT WHERE { ?s ?p ?o }")?;
    /// assert_eq!(
    ///     results.write_graph(Vec::new(), RdfFormat::NTriples)?,
    ///     graph.as_bytes()
    /// );
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub fn write_graph<W: Write>(
        self,
        writer: W,
        format: impl Into<RdfFormat>,
    ) -> Result<W, EvaluationError> {
        if let Self::Graph(triples) = self {
            let mut serializer = RdfSerializer::from_format(format.into()).for_writer(writer);
            for triple in triples {
                serializer
                    .serialize_triple(&triple?)
                    .map_err(EvaluationError::ResultsSerialization)?;
            }
            serializer
                .finish()
                .map_err(EvaluationError::ResultsSerialization)
        } else {
            Err(EvaluationError::NotAGraph)
        }
    }
}

impl From<QuerySolutionIter> for QueryResults {
    #[inline]
    fn from(value: QuerySolutionIter) -> Self {
        Self::Solutions(value)
    }
}

impl<R: Read + 'static> From<ReaderQueryResultsParserOutput<R>> for QueryResults {
    fn from(reader: ReaderQueryResultsParserOutput<R>) -> Self {
        match reader {
            ReaderQueryResultsParserOutput::Solutions(s) => Self::Solutions(s.into()),
            ReaderQueryResultsParserOutput::Boolean(v) => Self::Boolean(v),
        }
    }
}

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
pub struct QuerySolutionIter {}

impl QuerySolutionIter {
    /// Construct a new iterator of solution from an ordered list of solution variables and an iterator of solution tuples
    /// (each tuple using the same ordering as the variable list such that tuple element 0 is the value for the variable 0...)
    pub fn new(
        variables: Arc<[Variable]>,
        iter: impl Iterator<Item = Result<QuerySolution, EvaluationError>> + 'static,
    ) -> Self {
        unimplemented!()
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
        unimplemented!()
    }
}

impl<R: Read + 'static> From<ReaderSolutionsParser<R>> for QuerySolutionIter {
    fn from(reader: ReaderSolutionsParser<R>) -> Self {
        Self::new(
            reader.variables().into(),
            Box::new(
                reader.map(|t| t.map_err(|e| SparqlEvaluationError::Service(Box::new(e)).into())),
            ),
        )
    }
}

impl Iterator for QuerySolutionIter {
    type Item = Result<QuerySolution, EvaluationError>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        unimplemented!()
    }
}

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
pub struct QueryTripleIter {}

impl Iterator for QueryTripleIter {
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

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;
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
                QueryResults::Solutions(QuerySolutionIter::new(
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
