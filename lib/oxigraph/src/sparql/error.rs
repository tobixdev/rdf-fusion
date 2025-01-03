use crate::io::RdfParseError;
use crate::model::NamedNode;
use crate::sparql::results::QueryResultsParseError as ResultsParseError;
use crate::sparql::SparqlSyntaxError;
use oxiri::IriParseError;
use oxrdf::Term;
use oxrdfio::RdfFormat;
use std::convert::Infallible;
use std::error::Error;
use std::io;

/// A SPARQL evaluation error
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum QueryEvaluationError {
    /// Error from the underlying RDF dataset
    #[error(transparent)]
    Dataset(Box<dyn Error + Send + Sync>),
    /// Error during `SERVICE` evaluation
    #[error("{0}")]
    Service(#[source] Box<dyn Error + Send + Sync>),
    /// Error if the dataset returns the default graph even if a named graph is expected
    #[error("The SPARQL dataset returned the default graph even if a named graph is expected")]
    UnexpectedDefaultGraph,
    /// The variable storing the `SERVICE` name is unbound
    #[error("The variable encoding the service name is unbound")]
    UnboundService,
    /// Invalid service name
    #[error("{0} is not a valid service name")]
    InvalidServiceName(Term),
    /// The given `SERVICE` is not supported
    #[error("The service {0} is not supported")]
    UnsupportedService(NamedNode),
    #[error("The storage provided a triple term that is not a valid RDF-star term")]
    InvalidStorageTripleTerm,
}

impl From<Infallible> for QueryEvaluationError {
    #[inline]
    fn from(error: Infallible) -> Self {
        match error {}
    }
}

/// A SPARQL evaluation error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EvaluationError {
    /// An error in SPARQL parsing.
    #[error(transparent)]
    Parsing(#[from] SparqlSyntaxError),
    /// An error from the storage.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// An error while parsing an external RDF file.
    #[error(transparent)]
    GraphParsing(#[from] RdfParseError),
    /// An error while parsing an external result file (likely from a federated query).
    #[error(transparent)]
    ResultsParsing(#[from] ResultsParseError),
    /// An error returned during results serialization.
    #[error(transparent)]
    ResultsSerialization(io::Error),
    /// Error during `SERVICE` evaluation
    #[error("{0}")]
    Service(#[source] Box<dyn Error + Send + Sync + 'static>),
    /// Error when `CREATE` tries to create an already existing graph
    #[error("The graph {0} already exists")]
    GraphAlreadyExists(NamedNode),
    /// Error when `DROP` or `CLEAR` tries to remove a not existing graph
    #[error("The graph {0} does not exist")]
    GraphDoesNotExist(NamedNode),
    /// The variable storing the `SERVICE` name is unbound
    #[error("The variable encoding the service name is unbound")]
    UnboundService,
    /// The given `SERVICE` is not supported
    #[error("The service {0} is not supported")]
    UnsupportedService(NamedNode),
    /// The given content media type returned from an HTTP response is not supported (`SERVICE` and `LOAD`)
    #[error("The content media type {0} is not supported")]
    UnsupportedContentType(String),
    /// The `SERVICE` call has not returns solutions
    #[error("The service is not returning solutions but a boolean or a graph")]
    ServiceDoesNotReturnSolutions,
    /// The results are not a RDF graph
    #[error("The query results are not a RDF graph")]
    NotAGraph,
    #[doc(hidden)]
    #[error(transparent)]
    Unexpected(Box<dyn Error + Send + Sync>),
}

impl From<Infallible> for EvaluationError {
    #[inline]
    fn from(error: Infallible) -> Self {
        match error {}
    }
}

impl From<QueryEvaluationError> for EvaluationError {
    fn from(error: QueryEvaluationError) -> Self {
        match error {
            QueryEvaluationError::Dataset(error) => match error.downcast() {
                Ok(error) => Self::Storage(*error),
                Err(error) => Self::Unexpected(error),
            },
            QueryEvaluationError::Service(error) => Self::Service(error),
            QueryEvaluationError::UnexpectedDefaultGraph => Self::Storage(
                CorruptionError::new("Unexpected default graph in SPARQL results").into(),
            ),
            e => Self::Storage(
                CorruptionError::new(format!("Unsupported SPARQL evaluation error: {e}")).into(),
            ),
        }
    }
}

impl From<EvaluationError> for io::Error {
    #[inline]
    fn from(error: EvaluationError) -> Self {
        match error {
            EvaluationError::Parsing(error) => Self::new(io::ErrorKind::InvalidData, error),
            EvaluationError::GraphParsing(error) => error.into(),
            EvaluationError::ResultsParsing(error) => error.into(),
            EvaluationError::ResultsSerialization(error) => error,
            EvaluationError::Storage(error) => error.into(),
            EvaluationError::Service(error) | EvaluationError::Unexpected(error) => {
                match error.downcast() {
                    Ok(error) => *error,
                    Err(error) => Self::other(error),
                }
            }
            EvaluationError::GraphAlreadyExists(_)
            | EvaluationError::GraphDoesNotExist(_)
            | EvaluationError::UnboundService
            | EvaluationError::UnsupportedService(_)
            | EvaluationError::UnsupportedContentType(_)
            | EvaluationError::ServiceDoesNotReturnSolutions
            | EvaluationError::NotAGraph => Self::new(io::ErrorKind::InvalidInput, error),
        }
    }
}

/// An error related to storage operations (reads, writes...).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageError {
    /// Error from the OS I/O layer.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// Error related to data corruption.
    #[error(transparent)]
    Corruption(#[from] CorruptionError),
    #[doc(hidden)]
    #[error("{0}")]
    Other(#[source] Box<dyn Error + Send + Sync + 'static>),
}

impl From<StorageError> for io::Error {
    #[inline]
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Io(error) => error,
            StorageError::Corruption(error) => error.into(),
            StorageError::Other(error) => Self::other(error),
        }
    }
}

/// An error return if some content in the database is corrupted.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct CorruptionError(#[from] CorruptionErrorKind);

/// An error return if some content in the database is corrupted.
#[derive(Debug, thiserror::Error)]
enum CorruptionErrorKind {
    #[error("{0}")]
    Msg(String),
    #[error("{0}")]
    Other(#[source] Box<dyn Error + Send + Sync + 'static>),
}

impl CorruptionError {
    /// Builds an error from a printable error message.
    #[inline]
    pub(crate) fn new(error: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        Self(CorruptionErrorKind::Other(error.into()))
    }

    /// Builds an error from a printable error message.
    #[inline]
    pub(crate) fn msg(msg: impl Into<String>) -> Self {
        Self(CorruptionErrorKind::Msg(msg.into()))
    }
}

impl From<CorruptionError> for io::Error {
    #[inline]
    fn from(error: CorruptionError) -> Self {
        Self::new(io::ErrorKind::InvalidData, error)
    }
}

/// An error raised while loading a file into a [`Store`](crate::store::Store).
#[derive(Debug, thiserror::Error)]
pub enum LoaderError {
    /// An error raised while reading the file.
    #[error(transparent)]
    Parsing(#[from] RdfParseError),
    /// An error raised during the insertion in the store.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// The base IRI is invalid.
    #[error("Invalid base IRI '{iri}': {error}")]
    InvalidBaseIri {
        /// The IRI itself.
        iri: String,
        /// The parsing error.
        #[source]
        error: IriParseError,
    },
}

impl From<LoaderError> for io::Error {
    #[inline]
    fn from(error: LoaderError) -> Self {
        match error {
            LoaderError::Storage(error) => error.into(),
            LoaderError::Parsing(error) => error.into(),
            LoaderError::InvalidBaseIri { .. } => {
                Self::new(io::ErrorKind::InvalidInput, error.to_string())
            }
        }
    }
}

/// An error raised while writing a file from a [`Store`](crate::store::Store).
#[derive(Debug, thiserror::Error)]
pub enum SerializerError {
    /// An error raised while writing the content.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// An error raised during the lookup in the store.
    #[error(transparent)]
    Storage(#[from] StorageError),
    /// A format compatible with [RDF dataset](https://www.w3.org/TR/rdf11-concepts/#dfn-rdf-dataset) is required.
    #[error("A RDF format supporting datasets was expected, {0} found")]
    DatasetFormatExpected(RdfFormat),
}

impl From<SerializerError> for io::Error {
    #[inline]
    fn from(error: SerializerError) -> Self {
        match error {
            SerializerError::Storage(error) => error.into(),
            SerializerError::Io(error) => error,
            SerializerError::DatasetFormatExpected(_) => {
                Self::new(io::ErrorKind::InvalidInput, error.to_string())
            }
        }
    }
}
