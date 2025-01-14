use futures::{Stream, StreamExt};
use oxrdf::VariableRef;
use oxrdfio::{RdfFormat, RdfSerializer};
use sparesults::{
    QueryResultsFormat, QueryResultsParseError, QueryResultsParser, QueryResultsSerializer,
    ReaderQueryResultsParserOutput,
};
use std::io::{Read, Write};

mod graph_name;
mod quads;
mod query_solution;
mod triples;

use crate::sparql::error::EvaluationError;
pub use graph_name::GraphNameStream;
pub use quads::QuadStream;
pub use query_solution::QuerySolutionStream;
pub use sparesults::QuerySolution;
pub use triples::QueryTripleStream;

/// Results of a [SPARQL query](https://www.w3.org/TR/sparql11-query/).
pub enum QueryResults {
    /// Results of a [SELECT](https://www.w3.org/TR/sparql11-query/#select) query.
    Solutions(QuerySolutionStream),
    /// Result of a [ASK](https://www.w3.org/TR/sparql11-query/#ask) query.
    Boolean(bool),
    /// Results of a [CONSTRUCT](https://www.w3.org/TR/sparql11-query/#construct) or [DESCRIBE](https://www.w3.org/TR/sparql11-query/#describe) query.
    Graph(QueryTripleStream),
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
    pub async fn write<W: Write>(
        self,
        writer: W,
        format: QueryResultsFormat,
    ) -> Result<W, EvaluationError> {
        let serializer = QueryResultsSerializer::from_format(format);
        match self {
            Self::Boolean(value) => serializer.serialize_boolean_to_writer(writer, value),
            Self::Solutions(mut solutions) => {
                let mut serializer = serializer
                    .serialize_solutions_to_writer(writer, solutions.variables().to_vec())
                    .map_err(EvaluationError::ResultsSerialization)?;
                while let Some(solution) = solutions.next().await {
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

impl From<QuerySolutionStream> for QueryResults {
    #[inline]
    fn from(value: QuerySolutionStream) -> Self {
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
