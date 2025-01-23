use arrow_rdf::decoded::model::DecTerm;
use arrow_rdf::decoded::DecRdfTermBuilder;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::memory::MemoryStream;
use futures::StreamExt;
use oxrdf::{Variable, VariableRef};
use oxrdfio::{RdfFormat, RdfSerializer};
use sparesults::{
    QueryResultsFormat, QueryResultsParseError, QueryResultsParser, QueryResultsSerializer,
    ReaderQueryResultsParserOutput,
};
use std::error::Error;
use std::io::{Read, Write};
use std::sync::Arc;

mod decoding;
mod graph_name;
mod quads;
mod query_solution;
mod triples;

use crate::sparql::error::EvaluationError;
pub use decoding::decode_rdf_terms;
pub use decoding::DecodeRdfTermsToProjectionRule;
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
    ) -> Result<Self, QuerySolutionsToStreamError> {
        let parser = QueryResultsParser::from_format(format).for_reader(reader)?;
        query_result_for_parser(parser)
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

/// Indicates that there was a problem while turning a query result into a query solution stream.
#[derive(Debug, thiserror::Error)]
pub enum QuerySolutionsToStreamError {
    #[error("There was an error while obtaining the query solutions")]
    QuerySolutionSource(#[from] Box<dyn Error + Send + Sync>),
    #[error("Could not create a record batch from the result")]
    RecordBatchCreation(#[from] ArrowError),
    #[error("Could not create a stream from the resulting record batch")]
    StreamCreation(#[from] DataFusionError),
}

impl From<QueryResultsParseError> for QuerySolutionsToStreamError {
    fn from(value: QueryResultsParseError) -> Self {
        Self::QuerySolutionSource(Box::new(value))
    }
}

fn query_result_for_parser(
    parser: ReaderQueryResultsParserOutput<impl Read + Sized>,
) -> Result<QueryResults, QuerySolutionsToStreamError> {
    Ok(match parser {
        ReaderQueryResultsParserOutput::Solutions(s) => {
            let variables: Arc<[Variable]> = s.variables().into();
            let parser_iter = s
                .into_iter()
                .map(|r| r.map_err(|error| Box::new(error) as Box<dyn Error + Send + Sync>));
            query_result_for_iterator(variables, parser_iter)?
        }
        ReaderQueryResultsParserOutput::Boolean(v) => QueryResults::Boolean(v),
    })
}

pub fn query_result_for_iterator(
    variables: Arc<[Variable]>,
    solutions: impl Iterator<Item = Result<QuerySolution, Box<dyn Error + Send + Sync>>>,
) -> Result<QueryResults, QuerySolutionsToStreamError> {
    let mut builders = Vec::new();
    for _ in 0..variables.len() {
        builders.push(DecRdfTermBuilder::new())
    }

    for solution in solutions {
        let solution =
            solution.map_err(|err| QuerySolutionsToStreamError::QuerySolutionSource(err))?;
        for (idx, (_, term)) in solution.iter().enumerate() {
            builders
                .get_mut(idx)
                .expect("Initialized with enough builders")
                .append_term(term)?
        }
    }

    let fields = variables
        .iter()
        .map(|v| Field::new(v.as_str(), DecTerm::term_type(), true))
        .collect::<Vec<_>>();
    let columns = builders
        .into_iter()
        .map(|builder| builder.finish())
        .collect::<Result<Vec<_>, ArrowError>>()?;

    let schema = SchemaRef::new(Schema::new(fields));
    let record_batch = RecordBatch::try_new(schema.clone(), columns)?;
    let record_batch_stream = MemoryStream::try_new(vec![record_batch], schema, None)?;
    let stream = QuerySolutionStream::new(variables, Box::pin(record_batch_stream));
    Ok(QueryResults::Solutions(stream))
}

impl From<QuerySolutionStream> for QueryResults {
    #[inline]
    fn from(value: QuerySolutionStream) -> Self {
        Self::Solutions(value)
    }
}
