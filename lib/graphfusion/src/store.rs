//! API to access an on-disk [RDF dataset](https://www.w3.org/TR/rdf11-concepts/#dfn-rdf-dataset).
//!
//! The entry point of the module is the [`Store`] struct.
//!
//! Usage example:
//! ```
//! use graphfusion::model::*;
//! use graphfusion::sparql::QueryResults;
//! use graphfusion::store::Store;
//!
//! let store = Store::new()?;
//!
//! // insertion
//! let ex = NamedNode::new("http://example.com")?;
//! let quad = Quad::new(ex.clone(), ex.clone(), ex.clone(), GraphName::DefaultGraph);
//! store.insert(&quad)?;
//!
//! // quad filter
//! let results: Result<Vec<Quad>, _> = store.quads_for_pattern(None, None, None, None).collect();
//! assert_eq!(vec![quad], results?);
//!
//! // SPARQL query
//! if let QueryResults::Solutions(mut solutions) = store.query("SELECT ?s WHERE { ?s ?p ?o }")? {
//!     assert_eq!(solutions.next().unwrap()?.get("s"), Some(&ex.into()));
//! };
//! # Result::<_, Box<dyn std::error::Error>>::Ok(())
//! ```

use crate::error::{LoaderError, SerializerError};
use crate::io::{RdfParser, RdfSerializer};
use futures::StreamExt;
use graphfusion_engine::error::StorageError;
use graphfusion_engine::results::{GraphNameStream, QuadStream, QuerySolutionStream};
use graphfusion_engine::sparql::error::EvaluationError;
use graphfusion_engine::sparql::{
    Query, QueryExplanation, QueryOptions, QueryResults, Update, UpdateOptions,
};
use graphfusion_engine::TripleStore;
use graphfusion_store::MemoryTripleStore;
use once_cell::sync::Lazy;
use oxrdf::{
    GraphNameRef, NamedNodeRef, NamedOrBlankNodeRef, Quad, QuadRef, SubjectRef, TermRef, Variable,
};
use std::io::{Read, Write};
use std::sync::Arc;

static QUAD_VARIABLES: Lazy<Arc<[Variable]>> = Lazy::new(|| {
    Arc::new([
        Variable::new_unchecked("graph"),
        Variable::new_unchecked("subject"),
        Variable::new_unchecked("predicate"),
        Variable::new_unchecked("object"),
    ])
});

/// An on-disk [RDF dataset](https://www.w3.org/TR/rdf11-concepts/#dfn-rdf-dataset).
/// Allows to query and update it using SPARQL.
/// It is based on the [RocksDB](https://rocksdb.org/) key-value store.
///
/// This store ensures the "repeatable read" isolation level: the store only exposes changes that have
/// been "committed" (i.e. no partial writes) and the exposed state does not change for the complete duration
/// of a read operation (e.g. a SPARQL query) or a read/write operation (e.g. a SPARQL update).
///
/// Usage example:
/// ```
/// use graphfusion::model::*;
/// use graphfusion::sparql::QueryResults;
/// use graphfusion::store::Store;
/// # use std::fs::remove_dir_all;
///
/// # {
/// let store = Store::open("example.db")?;
///
/// // insertion
/// let ex = NamedNode::new("http://example.com")?;
/// let quad = Quad::new(ex.clone(), ex.clone(), ex.clone(), GraphName::DefaultGraph);
/// store.insert(&quad)?;
///
/// // quad filter
/// let results: Result<Vec<Quad>, _> = store.quads_for_pattern(None, None, None, None).collect();
/// assert_eq!(vec![quad], results?);
///
/// // SPARQL query
/// if let QueryResults::Solutions(mut solutions) = store.query("SELECT ?s WHERE { ?s ?p ?o }")? {
///     assert_eq!(solutions.next().unwrap()?.get("s"), Some(&ex.into()));
/// };
/// #
/// # };
/// # remove_dir_all("example.db")?;
/// # Result::<_, Box<dyn std::error::Error>>::Ok(())
/// ```
#[derive(Clone)]
pub struct Store {
    inner: Arc<dyn TripleStore + Send + Sync>,
}

impl Store {
    /// New in-memory [`Store`].
    pub async fn new() -> Result<Self, StorageError> {
        let inner = Arc::new(MemoryTripleStore::new().await?);
        Ok(Self { inner })
    }

    /// Executes a [SPARQL 1.1 query](https://www.w3.org/TR/sparql11-query/).
    ///
    /// Usage example:
    /// ```
    /// use graphfusion::model::*;
    /// use graphfusion::sparql::QueryResults;
    /// use graphfusion::store::Store;
    ///
    /// let store = Store::new()?;
    ///
    /// // insertions
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// store.insert(QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph))?;
    ///
    /// // SPARQL query
    /// if let QueryResults::Solutions(mut solutions) = store.query("SELECT ?s WHERE { ?s ?p ?o }")? {
    ///     assert_eq!(
    ///         solutions.next().unwrap()?.get("s"),
    ///         Some(&ex.into_owned().into())
    ///     );
    /// }
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub async fn query(
        &self,
        query: impl TryInto<Query, Error = impl Into<EvaluationError> + std::fmt::Debug>,
    ) -> Result<QueryResults, EvaluationError> {
        self.query_opt(query, QueryOptions::default()).await
    }

    /// Executes a [SPARQL 1.1 query](https://www.w3.org/TR/sparql11-query/) with some options.
    ///
    /// Usage example with a custom function serializing terms to N-Triples:
    /// ```
    /// use graphfusion::model::*;
    /// use graphfusion::sparql::{QueryOptions, QueryResults};
    /// use graphfusion::store::Store;
    ///
    /// let store = Store::new()?;
    /// if let QueryResults::Solutions(mut solutions) = store.query_opt(
    ///     "SELECT (<http://www.w3.org/ns/formats/N-Triples>(1) AS ?nt) WHERE {}",
    ///     QueryOptions::default().with_custom_function(
    ///         NamedNode::new("http://www.w3.org/ns/formats/N-Triples")?,
    ///         |args| args.get(0).map(|t| Literal::from(t.to_string()).into()),
    ///     ),
    /// )? {
    ///     assert_eq!(
    ///         solutions.next().unwrap()?.get("nt"),
    ///         Some(&Literal::from("\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>").into())
    ///     );
    /// }
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub async fn query_opt(
        &self,
        query: impl TryInto<Query, Error = impl Into<EvaluationError> + std::fmt::Debug>,
        options: QueryOptions,
    ) -> Result<QueryResults, EvaluationError> {
        self.explain_query_opt(query, options).await.map(|(r, _)| r)
    }

    /// Executes a [SPARQL 1.1 query](https://www.w3.org/TR/sparql11-query/) with some options and
    /// returns a query explanation with some statistics (if enabled with the `with_stats` parameter).
    ///
    /// <div class="warning">If you want to compute statistics you need to exhaust the results iterator before having a look at them.</div>
    ///
    /// Usage example serialising the explanation with statistics in JSON:
    /// ```
    /// use graphfusion::sparql::{QueryOptions, QueryResults};
    /// use graphfusion::store::Store;
    ///
    /// let store = Store::new()?;
    /// if let (Ok(QueryResults::Solutions(solutions)), explanation) = store.explain_query_opt(
    ///     "SELECT ?s WHERE { VALUES ?s { 1 2 3 } }",
    ///     QueryOptions::default(),
    ///     true,
    /// )? {
    ///     // We make sure to have read all the solutions
    ///     for _ in solutions {}
    ///     let mut buf = Vec::new();
    ///     explanation.write_in_json(&mut buf)?;
    /// }
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub async fn explain_query_opt(
        &self,
        query: impl TryInto<Query, Error = impl Into<EvaluationError> + std::fmt::Debug>,
        options: QueryOptions,
    ) -> Result<(QueryResults, Option<QueryExplanation>), EvaluationError> {
        let query = query.try_into();
        if query.is_err() {
            return Err(query.unwrap_err().into());
        }
        self.inner.execute_query(&query.unwrap(), options).await
    }

    /// Retrieves quads with a filter on each quad component
    ///
    /// Usage example:
    /// ```
    /// use graphfusion::model::*;
    /// use graphfusion::store::Store;
    ///
    /// let store = Store::new()?;
    ///
    /// // insertion
    /// let ex = NamedNode::new("http://example.com")?;
    /// let quad = Quad::new(ex.clone(), ex.clone(), ex.clone(), GraphName::DefaultGraph);
    /// store.insert(&quad)?;
    ///
    /// // quad filter by object
    /// let results = store
    ///     .quads_for_pattern(None, None, Some((&ex).into()), None)
    ///     .collect::<Result<Vec<_>, _>>()?;
    /// assert_eq!(vec![quad], results);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub async fn quads_for_pattern(
        &self,
        subject: Option<SubjectRef<'_>>,
        predicate: Option<NamedNodeRef<'_>>,
        object: Option<TermRef<'_>>,
        graph_name: Option<GraphNameRef<'_>>,
    ) -> Result<QuadStream, EvaluationError> {
        let record_batch_stream = self
            .inner
            .quads_for_pattern(graph_name, subject, predicate, object)
            .await?;
        let solution_stream = QuerySolutionStream::new(QUAD_VARIABLES.clone(), record_batch_stream);
        Ok(QuadStream::try_new(solution_stream).expect("Schema is guaranteed"))
    }

    /// Returns all the quads contained in the store.
    ///
    /// Usage example:
    /// ```
    /// use graphfusion::model::*;
    /// use graphfusion::store::Store;
    ///
    /// let store = Store::new()?;
    ///
    /// // insertion
    /// let ex = NamedNode::new("http://example.com")?;
    /// let quad = Quad::new(ex.clone(), ex.clone(), ex.clone(), GraphName::DefaultGraph);
    /// store.insert(&quad)?;
    ///
    /// // quad filter by object
    /// let results = store.iter().collect::<Result<Vec<_>, _>>()?;
    /// assert_eq!(vec![quad], results);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub async fn stream(&self) -> Result<QuadStream, EvaluationError> {
        let record_batch_stream = self
            .inner
            .quads_for_pattern(None, None, None, None)
            .await
            .map_err(EvaluationError::from)?;
        let solution_stream = QuerySolutionStream::new(QUAD_VARIABLES.clone(), record_batch_stream);
        Ok(QuadStream::try_new(solution_stream).expect("Schema guaranteed by TripleStore"))
    }

    /// Checks if this store contains a given quad.
    ///
    /// Usage example:
    /// ```
    /// use graphfusion::model::*;
    /// use graphfusion::store::Store;
    ///
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let quad = QuadRef::new(ex, ex, ex, ex);
    ///
    /// let store = Store::new()?;
    /// assert!(!store.contains(quad)?);
    ///
    /// store.insert(quad)?;
    /// assert!(store.contains(quad)?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub async fn contains<'a>(&self, quad: impl Into<QuadRef<'a>>) -> Result<bool, StorageError> {
        let quad = quad.into();
        self.inner.contains(&quad).await.map_err(StorageError::from)
    }

    /// Returns the number of quads in the store.
    ///
    /// <div class="warning">This function executes a full scan.</div>
    ///
    /// Usage example:
    /// ```
    /// use graphfusion::model::*;
    /// use graphfusion::store::Store;
    ///
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let store = Store::new()?;
    /// store.insert(QuadRef::new(ex, ex, ex, ex))?;
    /// store.insert(QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph))?;
    /// assert_eq!(2, store.len()?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub async fn len(&self) -> Result<usize, StorageError> {
        self.inner.len().await.map_err(StorageError::from)
    }

    /// Returns if the store is empty.
    ///
    /// Usage example:
    /// ```
    /// use graphfusion::model::*;
    /// use graphfusion::store::Store;
    ///
    /// let store = Store::new()?;
    /// assert!(store.is_empty()?);
    ///
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// store.insert(QuadRef::new(ex, ex, ex, ex))?;
    /// assert!(!store.is_empty()?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub fn is_empty(&self) -> Result<bool, StorageError> {
        unimplemented!()
    }

    /// Executes a [SPARQL 1.1 update](https://www.w3.org/TR/sparql11-update/).
    ///
    /// Usage example:
    /// ```
    /// use graphfusion::model::*;
    /// use graphfusion::store::Store;
    ///
    /// let store = Store::new()?;
    ///
    /// // insertion
    /// store
    ///     .update("INSERT DATA { <http://example.com> <http://example.com> <http://example.com> }")?;
    ///
    /// // we inspect the store contents
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// assert!(store.contains(QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph))?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub fn update(
        &self,
        _update: impl TryInto<Update, Error = impl Into<EvaluationError>>,
    ) -> Result<(), EvaluationError> {
        unimplemented!()
    }

    /// Executes a [SPARQL 1.1 update](https://www.w3.org/TR/sparql11-update/) with some options.
    ///
    /// ```
    /// use graphfusion::store::Store;
    /// use graphfusion::model::*;
    /// use graphfusion::sparql::QueryOptions;
    ///
    /// let store = Store::new()?;
    /// store.update_opt(
    ///     "INSERT { ?s <http://example.com/n-triples-representation> ?n } WHERE { ?s ?p ?o BIND(<http://www.w3.org/ns/formats/N-Triples>(?s) AS ?nt) }",
    ///     QueryOptions::default().with_custom_function(
    ///         NamedNode::new("http://www.w3.org/ns/formats/N-Triples")?,
    ///         |args| args.get(0).map(|t| Literal::from(t.to_string()).into())
    ///     )
    /// )?;
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub fn update_opt(
        &self,
        _update: impl TryInto<Update, Error = impl Into<EvaluationError>>,
        _options: impl Into<UpdateOptions>,
    ) -> Result<(), EvaluationError> {
        unimplemented!()
    }

    /// Loads a RDF file under into the store.
    ///
    /// This function is atomic, quite slow and memory hungry. To get much better performances you might want to use the [`bulk_loader`](Store::bulk_loader).
    ///
    /// Usage example:
    /// ```
    /// use graphfusion::store::Store;
    /// use graphfusion::io::RdfFormat;
    /// use graphfusion::model::*;
    /// use oxrdfio::RdfParser;
    ///
    /// let store = Store::new()?;
    ///
    /// // insert a dataset file (former load_dataset method)
    /// let file = b"<http://example.com> <http://example.com> <http://example.com> <http://example.com/g> .";
    /// store.load_from_reader(RdfFormat::NQuads, file.as_ref())?;
    ///
    /// // insert a graph file (former load_graph method)
    /// let file = b"<> <> <> .";
    /// store.load_from_reader(
    ///     RdfParser::from_format(RdfFormat::Turtle)
    ///         .with_base_iri("http://example.com")?
    ///         .without_named_graphs() // No named graphs allowed in the input
    ///         .with_default_graph(NamedNodeRef::new("http://example.com/g2")?), // we put the file default graph inside of a named graph
    ///     file.as_ref()
    /// )?;
    ///
    /// // we inspect the store contents
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// assert!(store.contains(QuadRef::new(ex, ex, ex, NamedNodeRef::new("http://example.com/g")?))?);
    /// assert!(store.contains(QuadRef::new(ex, ex, ex, NamedNodeRef::new("http://example.com/g2")?))?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub async fn load_from_reader(
        &self,
        parser: impl Into<RdfParser>,
        reader: impl Read,
    ) -> Result<(), LoaderError> {
        let quads = parser
            .into()
            .rename_blank_nodes()
            .for_reader(reader)
            .collect::<Result<Vec<_>, _>>()?;
        self.inner
            .load_quads(quads)
            .await
            .map(|_| ())
            .map_err(|err| LoaderError::from(StorageError::from(err)))
    }

    /// Adds a quad to this store.
    ///
    /// Returns `true` if the quad was not already in the store.
    ///
    /// Usage example:
    /// ```
    /// use graphfusion::model::*;
    /// use graphfusion::store::Store;
    ///
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let quad = QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph);
    ///
    /// let store = Store::new()?;
    /// assert!(store.insert(quad)?);
    /// assert!(!store.insert(quad)?);
    ///
    /// assert!(store.contains(quad)?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub async fn insert<'a>(&self, quad: impl Into<QuadRef<'a>>) -> Result<bool, StorageError> {
        let quad = vec![quad.into().into_owned()];
        self.inner
            .load_quads(quad)
            .await
            .map(|inserted| inserted > 0)
            .map_err(|err| StorageError::from(err))
    }

    /// Adds atomically a set of quads to this store.
    ///
    /// <div class="warning">
    ///
    /// This operation uses a memory heavy transaction internally, use the [`bulk_loader`](Store::bulk_loader) if you plan to add ten of millions of triples.</div>
    pub fn extend(
        &self,
        _quads: impl IntoIterator<Item = impl Into<Quad>>,
    ) -> Result<(), StorageError> {
        unimplemented!()
    }

    /// Removes a quad from this store.
    ///
    /// Returns `true` if the quad was in the store and has been removed.
    ///
    /// Usage example:
    /// ```
    /// use graphfusion::model::*;
    /// use graphfusion::store::Store;
    ///
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let quad = QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph);
    ///
    /// let store = Store::new()?;
    /// store.insert(quad)?;
    /// assert!(store.remove(quad)?);
    /// assert!(!store.remove(quad)?);
    ///
    /// assert!(!store.contains(quad)?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub async fn remove<'a>(&self, quad: impl Into<QuadRef<'a>>) -> Result<bool, StorageError> {
        self.inner
            .remove(quad.into())
            .await
            .map_err(|err| StorageError::from(err))
    }

    /// Dumps the store into a file.
    ///
    /// ```
    /// use graphfusion::io::RdfFormat;
    /// use graphfusion::store::Store;
    ///
    /// let file =
    ///     "<http://example.com> <http://example.com> <http://example.com> <http://example.com> .\n"
    ///         .as_bytes();
    ///
    /// let store = Store::new()?;
    /// store.load_from_reader(RdfFormat::NQuads, file)?;
    ///
    /// let buffer = store.dump_to_writer(RdfFormat::NQuads, Vec::new())?;
    /// assert_eq!(file, buffer.as_slice());
    /// # std::io::Result::Ok(())
    /// ```
    pub async fn dump_to_writer<W: Write>(
        &self,
        serializer: impl Into<RdfSerializer>,
        writer: W,
    ) -> Result<W, SerializerError> {
        let serializer = serializer.into();
        if !serializer.format().supports_datasets() {
            return Err(SerializerError::DatasetFormatExpected(serializer.format()));
        }
        let mut serializer = serializer.for_writer(writer);
        let mut stream = self.stream().await?;
        while let Some(quad) = stream.next().await {
            serializer.serialize_quad(&quad?)?;
        }
        Ok(serializer.finish()?)
    }

    /// Dumps a store graph into a file.
    ///
    /// Usage example:
    /// ```
    /// use graphfusion::io::RdfFormat;
    /// use graphfusion::model::*;
    /// use graphfusion::store::Store;
    ///
    /// let file = "<http://example.com> <http://example.com> <http://example.com> .\n".as_bytes();
    ///
    /// let store = Store::new()?;
    /// store.load_graph(file, RdfFormat::NTriples, GraphName::DefaultGraph, None)?;
    ///
    /// let mut buffer = Vec::new();
    /// store.dump_graph_to_writer(GraphNameRef::DefaultGraph, RdfFormat::NTriples, &mut buffer)?;
    /// assert_eq!(file, buffer.as_slice());
    /// # std::io::Result::Ok(())
    /// ```
    pub async fn dump_graph_to_writer<'a, W: Write>(
        &self,
        from_graph_name: impl Into<GraphNameRef<'a>>,
        serializer: impl Into<RdfSerializer>,
        writer: W,
    ) -> Result<W, SerializerError> {
        let mut serializer = serializer.into().for_writer(writer);
        let mut stream = self
            .quads_for_pattern(None, None, None, Some(from_graph_name.into()))
            .await?;
        while let Some(quad) = stream.next().await {
            serializer.serialize_triple(quad?.as_ref())?;
        }
        Ok(serializer.finish()?)
    }

    /// Returns all the store named graphs.
    ///
    /// Usage example:
    /// ```
    /// use graphfusion::model::*;
    /// use graphfusion::store::Store;
    ///
    /// let ex = NamedNode::new("http://example.com")?;
    /// let store = Store::new()?;
    /// store.insert(QuadRef::new(&ex, &ex, &ex, &ex))?;
    /// store.insert(QuadRef::new(&ex, &ex, &ex, GraphNameRef::DefaultGraph))?;
    /// assert_eq!(
    ///     vec![NamedOrBlankNode::from(ex)],
    ///     store.named_graphs().collect::<Result<Vec<_>, _>>()?
    /// );
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub fn named_graphs(&self) -> GraphNameStream {
        unimplemented!()
    }

    /// Checks if the store contains a given graph
    ///
    /// Usage example:
    /// ```
    /// use graphfusion::model::{NamedNode, QuadRef};
    /// use graphfusion::store::Store;
    ///
    /// let ex = NamedNode::new("http://example.com")?;
    /// let store = Store::new()?;
    /// store.insert(QuadRef::new(&ex, &ex, &ex, &ex))?;
    /// assert!(store.contains_named_graph(&ex)?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub fn contains_named_graph<'a>(
        &self,
        _graph_name: impl Into<NamedOrBlankNodeRef<'a>>,
    ) -> Result<bool, StorageError> {
        unimplemented!()
    }

    /// Inserts a graph into this store.
    ///
    /// Returns `true` if the graph was not already in the store.
    ///
    /// Usage example:
    /// ```
    /// use graphfusion::model::NamedNodeRef;
    /// use graphfusion::store::Store;
    ///
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let store = Store::new()?;
    /// store.insert_named_graph(ex)?;
    ///
    /// assert_eq!(
    ///     store.named_graphs().collect::<Result<Vec<_>, _>>()?,
    ///     vec![ex.into_owned().into()]
    /// );
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub fn insert_named_graph<'a>(
        &self,
        _graph_name: impl Into<NamedOrBlankNodeRef<'a>>,
    ) -> Result<bool, StorageError> {
        unimplemented!()
    }

    /// Clears a graph from this store.
    ///
    /// Usage example:
    /// ```
    /// use graphfusion::model::{NamedNodeRef, QuadRef};
    /// use graphfusion::store::Store;
    ///
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let quad = QuadRef::new(ex, ex, ex, ex);
    /// let store = Store::new()?;
    /// store.insert(quad)?;
    /// assert_eq!(1, store.len()?);
    ///
    /// store.clear_graph(ex)?;
    /// assert!(store.is_empty()?);
    /// assert_eq!(1, store.named_graphs().count());
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub fn clear_graph<'a>(
        &self,
        _graph_name: impl Into<GraphNameRef<'a>>,
    ) -> Result<(), StorageError> {
        unimplemented!()
    }

    /// Removes a graph from this store.
    ///
    /// Returns `true` if the graph was in the store and has been removed.
    ///
    /// Usage example:
    /// ```
    /// use graphfusion::model::{NamedNodeRef, QuadRef};
    /// use graphfusion::store::Store;
    ///
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let quad = QuadRef::new(ex, ex, ex, ex);
    /// let store = Store::new()?;
    /// store.insert(quad)?;
    /// assert_eq!(1, store.len()?);
    ///
    /// assert!(store.remove_named_graph(ex)?);
    /// assert!(store.is_empty()?);
    /// assert_eq!(0, store.named_graphs().count());
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub fn remove_named_graph<'a>(
        &self,
        _graph_name: impl Into<NamedOrBlankNodeRef<'a>>,
    ) -> Result<bool, StorageError> {
        unimplemented!()
    }

    /// Clears the store.
    ///
    /// Usage example:
    /// ```
    /// use graphfusion::model::*;
    /// use graphfusion::store::Store;
    ///
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let store = Store::new()?;
    /// store.insert(QuadRef::new(ex, ex, ex, ex))?;
    /// store.insert(QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph))?;
    /// assert_eq!(2, store.len()?);
    ///
    /// store.clear()?;
    /// assert!(store.is_empty()?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub fn clear(&self) -> Result<(), StorageError> {
        unimplemented!()
    }

    /// Validates that all the store invariants held in the data
    #[doc(hidden)]
    pub fn validate(&self) -> Result<(), StorageError> {
        // TODO: Is there anything we should do here?
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;
    use oxrdf::{BlankNode, GraphName, Literal, NamedNode, Subject, Term};

    #[test]
    fn test_send_sync() {
        fn is_send_sync<T: Send + Sync>() {}
        is_send_sync::<Store>();
    }

    #[tokio::test]
    async fn store() -> Result<(), EvaluationError> {
        let main_s = Subject::from(BlankNode::default());
        let main_p = NamedNode::new("http://example.com").unwrap();
        let main_o = Term::from(Literal::from(1));
        let main_g = GraphName::from(BlankNode::default());

        let default_quad = Quad::new(
            main_s.clone(),
            main_p.clone(),
            main_o.clone(),
            GraphName::DefaultGraph,
        );
        let named_quad = Quad::new(
            main_s.clone(),
            main_p.clone(),
            main_o.clone(),
            main_g.clone(),
        );
        let mut default_quads = vec![
            Quad::new(
                main_s.clone(),
                main_p.clone(),
                Literal::from(0),
                GraphName::DefaultGraph,
            ),
            default_quad.clone(),
            Quad::new(
                main_s.clone(),
                main_p.clone(),
                Literal::from(200_000_000),
                GraphName::DefaultGraph,
            ),
        ];
        let all_quads = vec![
            named_quad.clone(),
            Quad::new(
                main_s.clone(),
                main_p.clone(),
                Literal::from(200_000_000),
                GraphName::DefaultGraph,
            ),
            default_quad.clone(),
            Quad::new(
                main_s.clone(),
                main_p.clone(),
                Literal::from(0),
                GraphName::DefaultGraph,
            ),
        ];

        let store = Store::new().await?;
        for t in &default_quads {
            assert!(store.insert(t).await?);
        }
        assert!(!store.insert(&default_quad).await?);

        assert!(store.remove(&default_quad).await?);
        assert!(!store.remove(&default_quad).await?);
        assert!(store.insert(&named_quad).await?);
        assert!(!store.insert(&named_quad).await?);
        assert!(store.insert(&default_quad).await?);
        assert!(!store.insert(&default_quad).await?);
        store.validate()?;

        assert_eq!(store.len().await?, 4);
        assert_eq!(store.stream().await?.try_collect().await?, all_quads);
        assert_eq!(
            store
                .quads_for_pattern(Some(main_s.as_ref()), None, None, None)
                .await?
                .try_collect()
                .await?,
            all_quads
        );
        assert_eq!(
            store
                .quads_for_pattern(Some(main_s.as_ref()), Some(main_p.as_ref()), None, None)
                .await?
                .try_collect()
                .await?,
            all_quads
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    Some(main_s.as_ref()),
                    Some(main_p.as_ref()),
                    Some(main_o.as_ref()),
                    None
                )
                .await?
                .try_collect()
                .await?,
            vec![named_quad.clone(), default_quad.clone()]
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    Some(main_s.as_ref()),
                    Some(main_p.as_ref()),
                    Some(main_o.as_ref()),
                    Some(GraphNameRef::DefaultGraph)
                )
                .await?
                .try_collect()
                .await?,
            vec![default_quad.clone()]
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    Some(main_s.as_ref()),
                    Some(main_p.as_ref()),
                    Some(main_o.as_ref()),
                    Some(main_g.as_ref())
                )
                .await?
                .try_collect()
                .await?,
            vec![named_quad.clone()]
        );
        default_quads.reverse();
        assert_eq!(
            store
                .quads_for_pattern(
                    Some(main_s.as_ref()),
                    Some(main_p.as_ref()),
                    None,
                    Some(GraphNameRef::DefaultGraph)
                )
                .await?
                .try_collect()
                .await?,
            default_quads
        );
        assert_eq!(
            store
                .quads_for_pattern(Some(main_s.as_ref()), None, Some(main_o.as_ref()), None)
                .await?
                .try_collect()
                .await?,
            vec![named_quad.clone(), default_quad.clone()]
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    Some(main_s.as_ref()),
                    None,
                    Some(main_o.as_ref()),
                    Some(GraphNameRef::DefaultGraph)
                )
                .await?
                .try_collect()
                .await?,
            vec![default_quad.clone()]
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    Some(main_s.as_ref()),
                    None,
                    Some(main_o.as_ref()),
                    Some(main_g.as_ref())
                )
                .await?
                .try_collect()
                .await?,
            vec![named_quad.clone()]
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    Some(main_s.as_ref()),
                    None,
                    None,
                    Some(GraphNameRef::DefaultGraph)
                )
                .await?
                .try_collect()
                .await?,
            default_quads
        );
        assert_eq!(
            store
                .quads_for_pattern(None, Some(main_p.as_ref()), None, None)
                .await?
                .try_collect()
                .await?,
            all_quads
        );
        assert_eq!(
            store
                .quads_for_pattern(None, Some(main_p.as_ref()), Some(main_o.as_ref()), None)
                .await?
                .try_collect()
                .await?,
            vec![named_quad.clone(), default_quad.clone()]
        );
        assert_eq!(
            store
                .quads_for_pattern(None, None, Some(main_o.as_ref()), None)
                .await?
                .try_collect()
                .await?,
            vec![named_quad.clone(), default_quad.clone()]
        );
        assert_eq!(
            store
                .quads_for_pattern(None, None, None, Some(GraphNameRef::DefaultGraph))
                .await?
                .try_collect()
                .await?,
            default_quads
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    None,
                    Some(main_p.as_ref()),
                    Some(main_o.as_ref()),
                    Some(GraphNameRef::DefaultGraph)
                )
                .await?
                .try_collect()
                .await?,
            vec![default_quad]
        );
        assert_eq!(
            store
                .quads_for_pattern(
                    None,
                    Some(main_p.as_ref()),
                    Some(main_o.as_ref()),
                    Some(main_g.as_ref())
                )
                .await?
                .try_collect()
                .await?,
            vec![named_quad]
        );

        Ok(())
    }
}
