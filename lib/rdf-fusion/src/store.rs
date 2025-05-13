//! API to access an on-disk [RDF dataset](https://www.w3.org/TR/rdf11-concepts/#dfn-rdf-dataset).
//!
//! The entry point of the module is the [`Store`] struct.
//!
//! Usage example:
//! ```
//! use rdf_fusion::model::*;
//! use rdf_fusion::sparql::QueryResults;
//! use rdf_fusion::store::Store;
//! use futures::StreamExt;
//!
//! # tokio_test::block_on(async {
//! let store = Store::new();
//!
//! // insertion
//! let ex = NamedNode::new("http://example.com")?;
//! let quad = Quad::new(ex.clone(), ex.clone(), ex.clone(), GraphName::DefaultGraph);
//! store.insert(&quad).await?;
//!
//! // quad filter
//! let results = store.quads_for_pattern(None, None, None, None).await?
//!     .try_collect_to_vec().await?;
//! assert_eq!(vec![quad], results);
//!
//! // SPARQL query
//! if let QueryResults::Solutions(mut solutions) = store.query("SELECT ?s WHERE { ?s ?p ?o }").await? {
//!     assert_eq!(solutions.next().await.unwrap()?.get("s"), Some(&ex.into()));
//! };
//! # Result::<_, Box<dyn std::error::Error>>::Ok(())
//! # }).unwrap();
//! ```

use crate::error::{LoaderError, SerializerError};
use crate::sparql::error::QueryEvaluationError;
use futures::StreamExt;
use rdf_fusion_engine::error::StorageError;
use rdf_fusion_engine::results::{QuadStream, QuerySolutionStream};
use rdf_fusion_engine::sparql::{
    Query, QueryExplanation, QueryOptions, QueryResults, Update, UpdateOptions,
};
use rdf_fusion_engine::RdfFusionInstance;
use rdf_fusion_model::{
    GraphNameRef, NamedNodeRef, NamedOrBlankNode, NamedOrBlankNodeRef, Quad, QuadRef, SubjectRef,
    TermRef, Variable,
};
use rdf_fusion_storage::MemoryQuadStorage;
use oxrdfio::{RdfParser, RdfSerializer};
use std::io::{Read, Write};
use std::sync::{Arc, LazyLock};

static QUAD_VARIABLES: LazyLock<Arc<[Variable]>> = LazyLock::new(|| {
    Arc::new([
        Variable::new_unchecked("graph"),
        Variable::new_unchecked("subject"),
        Variable::new_unchecked("predicate"),
        Variable::new_unchecked("object"),
    ])
});

/// An [RDF dataset](https://www.w3.org/TR/rdf11-concepts/#dfn-rdf-dataset) store.
///
/// The store can be updated and queried using [SPARQL](https://www.w3.org/TR/sparql11-query).
///
/// Usage example:
/// ```
/// use rdf_fusion::model::*;
/// use rdf_fusion::sparql::QueryResults;
/// use rdf_fusion::store::Store;
/// use futures::StreamExt;
///
/// # tokio_test::block_on(async {
/// let store = Store::new();
///
/// // insertion
/// let ex = NamedNode::new("http://example.com")?;
/// let quad = Quad::new(ex.clone(), ex.clone(), ex.clone(), GraphName::DefaultGraph);
/// store.insert(&quad).await?;
///
/// // quad filter
/// let results = store.quads_for_pattern(None, None, None, None).await?.try_collect_to_vec().await?;
/// assert_eq!(vec![quad], results);
///
/// // SPARQL query
/// if let QueryResults::Solutions(mut solutions) = store.query("SELECT ?s WHERE { ?s ?p ?o }").await? {
///     assert_eq!(solutions.next().await.unwrap()?.get("s"), Some(&ex.into()));
/// };
///
/// Result::<_, Box<dyn std::error::Error>>::Ok(())
/// # }).unwrap();
/// ```
#[derive(Clone)]
pub struct Store {
    engine: RdfFusionInstance,
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

impl Store {
    /// Creates a [Store] with a [MemoryQuadStorage] as backing storage.
    #[allow(clippy::expect_used)]
    pub fn new() -> Store {
        let storage = MemoryQuadStorage::new("memory_quads");
        let engine = RdfFusionInstance::new_with_storage(Arc::new(storage))
            .expect("Name of the storage is OK");
        Self { engine }
    }

    /// Executes a [SPARQL](https://www.w3.org/TR/sparql11-query/) query.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::sparql::QueryResults;
    /// use rdf_fusion::store::Store;
    /// use futures::StreamExt;
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new();
    ///
    /// // insertions
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// store.insert(QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph)).await?;
    ///
    /// // SPARQL query
    /// if let QueryResults::Solutions(mut solutions) = store.query("SELECT ?s WHERE { ?s ?p ?o }").await? {
    ///     assert_eq!(
    ///         solutions.next().await.unwrap()?.get("s"),
    ///         Some(&ex.into_owned().into())
    ///     );
    /// }
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn query(
        &self,
        query: impl TryInto<Query, Error = impl Into<QueryEvaluationError> + std::fmt::Debug>,
    ) -> Result<QueryResults, QueryEvaluationError> {
        self.query_opt(query, QueryOptions).await
    }

    /// Executes a [SPARQL 1.1 query](https://www.w3.org/TR/sparql11-query/) with some options.
    ///
    /// Usage example with a custom function serializing terms to N-Triples:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::sparql::{QueryOptions, QueryResults};
    /// use rdf_fusion::store::Store;
    /// use futures::StreamExt;
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new();
    /// if let QueryResults::Solutions(mut solutions) = store.query_opt(
    ///     "SELECT (STR(1) AS ?nt) WHERE {}",
    ///     QueryOptions::default(),
    /// ).await? {
    ///     assert_eq!(
    ///         solutions.next().await.unwrap()?.get("nt"),
    ///         Some(&Literal::from("1").into())
    ///     );
    /// }
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn query_opt(
        &self,
        query: impl TryInto<Query, Error = impl Into<QueryEvaluationError> + std::fmt::Debug>,
        options: QueryOptions,
    ) -> Result<QueryResults, QueryEvaluationError> {
        self.explain_query_opt(query, options).await.map(|(r, _)| r)
    }

    /// Executes a [SPARQL 1.1 query](https://www.w3.org/TR/sparql11-query/) with some options and
    /// returns a query explanation with some statistics (if enabled with the `with_stats` parameter).
    ///
    /// <div class="warning">If you want to compute statistics you need to exhaust the results iterator before having a look at them.</div>
    ///
    /// Usage example serialising the explanation with statistics in JSON:
    /// ```
    /// use rdf_fusion::sparql::{QueryOptions, QueryResults};
    /// use rdf_fusion::store::Store;
    /// use futures::StreamExt;
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new();
    /// if let (QueryResults::Solutions(mut solutions), _explanation) = store.explain_query_opt(
    ///     "SELECT ?s WHERE { VALUES ?s { 1 2 3 } }",
    ///     QueryOptions::default(),
    /// ).await? {
    ///     // We make sure to have read all the solutions
    ///     while let Some(_) = solutions.next().await { }
    ///     // TODO
    ///     // let mut buf = Vec::new();
    ///     // explanation.write_in_json(&mut buf)?;
    /// }
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn explain_query_opt(
        &self,
        query: impl TryInto<Query, Error = impl Into<QueryEvaluationError> + std::fmt::Debug>,
        options: QueryOptions,
    ) -> Result<(QueryResults, Option<QueryExplanation>), QueryEvaluationError> {
        let query = query.try_into();
        match query {
            Ok(query) => self.engine.execute_query(&query, options).await,
            Err(err) => Err(err.into()),
        }
    }

    /// Retrieves quads with a filter on each quad component
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new();
    ///
    /// // insertion
    /// let ex = NamedNode::new("http://example.com")?;
    /// let quad = Quad::new(ex.clone(), ex.clone(), ex.clone(), GraphName::DefaultGraph);
    /// store.insert(&quad).await?;
    ///
    /// // quad filter by object
    /// let results = store
    ///     .quads_for_pattern(None, None, Some((&ex).into()), None).await?
    ///     .try_collect_to_vec().await?;
    /// assert_eq!(vec![quad], results);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn quads_for_pattern(
        &self,
        subject: Option<SubjectRef<'_>>,
        predicate: Option<NamedNodeRef<'_>>,
        object: Option<TermRef<'_>>,
        graph_name: Option<GraphNameRef<'_>>,
    ) -> Result<QuadStream, QueryEvaluationError> {
        let record_batch_stream = self
            .engine
            .quads_for_pattern(graph_name, subject, predicate, object)
            .await?;
        let solution_stream = QuerySolutionStream::new(QUAD_VARIABLES.clone(), record_batch_stream);
        QuadStream::try_new(solution_stream).map_err(QueryEvaluationError::InternalError)
    }

    /// Returns all the quads contained in the store.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new();
    ///
    /// // insertion
    /// let ex = NamedNode::new("http://example.com")?;
    /// let quad = Quad::new(ex.clone(), ex.clone(), ex.clone(), GraphName::DefaultGraph);
    /// store.insert(&quad).await?;
    ///
    /// // quad filter by object
    /// let results = store.stream().await?.try_collect_to_vec().await?;
    /// assert_eq!(vec![quad], results);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn stream(&self) -> Result<QuadStream, QueryEvaluationError> {
        let record_batch_stream = self
            .engine
            .quads_for_pattern(None, None, None, None)
            .await
            .map_err(QueryEvaluationError::from)?;
        let solution_stream = QuerySolutionStream::new(QUAD_VARIABLES.clone(), record_batch_stream);
        QuadStream::try_new(solution_stream).map_err(QueryEvaluationError::InternalError)
    }

    /// Checks if this store contains a given quad.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let quad = QuadRef::new(ex, ex, ex, ex);
    ///
    /// let store = Store::new();
    /// assert!(!store.contains(quad).await?);
    ///
    /// store.insert(quad).await?;
    /// assert!(store.contains(quad).await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn contains<'a>(
        &self,
        quad: impl Into<QuadRef<'a>>,
    ) -> Result<bool, QueryEvaluationError> {
        let quad = quad.into();
        self.engine
            .contains(&quad)
            .await
            .map_err(QueryEvaluationError::from)
    }

    /// Returns the number of quads in the store.
    ///
    /// <div class="warning">This function executes a full scan.</div>
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let store = Store::new();
    /// store.insert(QuadRef::new(ex, ex, ex, ex)).await?;
    /// store.insert(QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph)).await?;
    /// assert_eq!(2, store.len().await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn len(&self) -> Result<usize, QueryEvaluationError> {
        self.engine.len().await.map_err(QueryEvaluationError::from)
    }

    /// Returns if the store is empty.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new();
    /// assert!(store.is_empty().await?);
    ///
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// store.insert(QuadRef::new(ex, ex, ex, ex)).await?;
    /// assert!(!store.is_empty().await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn is_empty(&self) -> Result<bool, QueryEvaluationError> {
        Ok(self.len().await? == 0)
    }

    /// Executes a [SPARQL 1.1 update](https://www.w3.org/TR/sparql11-update/).
    ///
    /// Usage example:
    /// ```
    /// // use rdf-fusion::model::*;
    /// // use rdf-fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// // TODO #7: Implement Update
    /// // let store = Store::new();
    /// // insertion
    /// // store
    /// //    .update("INSERT DATA { <http://example.com> <http://example.com> <http://example.com> }").await?;
    ///
    /// // we inspect the store contents
    /// // let ex = NamedNodeRef::new("http://example.com")?;
    /// // assert!(store.contains(QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph)).await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    #[allow(clippy::unimplemented, reason = "Not production ready")]
    #[allow(clippy::unused_self, reason = "Not implemented")]
    #[allow(clippy::unused_async, reason = "Not implemented")]
    pub async fn update(
        &self,
        _update: impl TryInto<Update, Error = impl Into<QueryEvaluationError>>,
    ) -> Result<(), QueryEvaluationError> {
        unimplemented!()
    }

    /// Executes a [SPARQL 1.1 update](https://www.w3.org/TR/sparql11-update/) with some options.
    ///
    /// ```
    /// // use rdf-fusion::store::Store;
    /// // use rdf-fusion::sparql::QueryOptions;
    ///
    /// # tokio_test::block_on(async {
    /// // TODO #7: Implement Update
    /// // let store = Store::new();
    /// // store.update_opt(
    /// //    "INSERT { ?s <http://example.com/n-triples-representation> ?n } WHERE { ?s ?p ?o BIND(<http://www.w3.org/ns/formats/N-Triples>(?s) AS ?nt) }",
    /// //    QueryOptions::default()
    /// //).await?;
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    #[allow(clippy::unimplemented, reason = "Not production ready")]
    #[allow(clippy::unused_self, reason = "Not implemented")]
    #[allow(clippy::unused_async, reason = "Not implemented")]
    pub async fn update_opt(
        &self,
        _update: impl TryInto<Update, Error = impl Into<QueryEvaluationError>>,
        _options: impl Into<UpdateOptions>,
    ) -> Result<(), QueryEvaluationError> {
        unimplemented!()
    }

    /// Loads a RDF file under into the store.
    ///
    /// This function is atomic, quite slow and memory hungry. To get much better performances you might want to use the [`bulk_loader`](Store::bulk_loader).
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::store::Store;
    /// use rdf_fusion::io::RdfFormat;
    /// use rdf_fusion::model::*;
    /// use oxrdfio::RdfParser;
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new();
    ///
    /// // insert a dataset file (former load_dataset method)
    /// let file = b"<http://example.com> <http://example.com> <http://example.com> <http://example.com/g> .";
    /// store.load_from_reader(RdfFormat::NQuads, file.as_ref()).await?;
    ///
    /// // insert a graph file (former load_graph method)
    /// let file = b"<> <> <> .";
    /// store.load_from_reader(
    ///     RdfParser::from_format(RdfFormat::Turtle)
    ///         .with_base_iri("http://example.com")?
    ///         .without_named_graphs() // No named graphs allowed in the input
    ///         .with_default_graph(NamedNodeRef::new("http://example.com/g2")?), // we put the file default graph inside of a named graph
    ///     file.as_ref()
    /// ).await?;
    ///
    /// // we inspect the store contents
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// assert!(store.contains(QuadRef::new(ex, ex, ex, NamedNodeRef::new("http://example.com/g")?)).await?);
    /// assert!(store.contains(QuadRef::new(ex, ex, ex, NamedNodeRef::new("http://example.com/g2")?)).await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
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
        self.engine
            .storage()
            .extend(quads)
            .await
            .map(|_| ())
            .map_err(LoaderError::from)
    }

    /// Adds a quad to this store.
    ///
    /// Returns `true` if the quad was not already in the store.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let quad = QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph);
    ///
    /// let store = Store::new();
    /// assert!(store.insert(quad).await?);
    /// assert!(!store.insert(quad).await?);
    ///
    /// assert!(store.contains(quad).await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn insert<'a>(&self, quad: impl Into<QuadRef<'a>>) -> Result<bool, StorageError> {
        let quad = vec![quad.into().into_owned()];
        self.engine
            .storage()
            .extend(quad)
            .await
            .map(|inserted| inserted > 0)
    }

    /// Atomically adds a set of quads to this store.
    pub async fn extend(
        &self,
        quads: impl IntoIterator<Item = impl Into<Quad>>,
    ) -> Result<(), StorageError> {
        let quads = quads.into_iter().map(Into::into).collect::<Vec<_>>();
        self.engine.storage().extend(quads).await?;
        Ok(())
    }

    /// Removes a quad from this store.
    ///
    /// Returns `true` if the quad was in the store and has been removed.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let quad = QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph);
    ///
    /// let store = Store::new();
    /// store.insert(quad).await?;
    /// assert!(store.remove(quad).await?);
    /// assert!(!store.remove(quad).await?);
    ///
    /// assert!(!store.contains(quad).await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn remove<'a>(&self, quad: impl Into<QuadRef<'a>>) -> Result<bool, StorageError> {
        self.engine.storage().remove(quad.into()).await
    }

    /// Dumps the store into a file.
    ///
    /// ```
    /// use rdf_fusion::io::RdfFormat;
    /// use rdf_fusion::store::Store;
    ///
    /// let file =
    ///     "<http://example.com> <http://example.com> <http://example.com> <http://example.com> .\n"
    ///         .as_bytes();
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new();
    /// store.load_from_reader(RdfFormat::NQuads, file).await?;
    ///
    /// let buffer = store.dump_to_writer(RdfFormat::NQuads, Vec::new()).await?;
    /// assert_eq!(file, buffer.as_slice());
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
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
    /// use oxrdfio::RdfParser;
    /// use rdf_fusion::io::RdfFormat;
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// let file = "<http://example.com> <http://example.com> <http://example.com> .\n".as_bytes();
    ///
    /// # tokio_test::block_on(async {
    /// let store = Store::new();
    /// let parser = RdfParser::from_format(RdfFormat::NTriples);
    /// store.load_from_reader(parser, file.as_ref()).await?;
    ///
    /// let mut buffer = Vec::new();
    /// store.dump_graph_to_writer(GraphNameRef::DefaultGraph, RdfFormat::NTriples, &mut buffer).await?;
    /// assert_eq!(file, buffer.as_slice());
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
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
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNode::new("http://example.com")?;
    /// let store = Store::new();
    /// store.insert(QuadRef::new(&ex, &ex, &ex, &ex)).await?;
    /// store.insert(QuadRef::new(&ex, &ex, &ex, GraphNameRef::DefaultGraph)).await?;
    /// assert_eq!(
    ///     vec![NamedOrBlankNode::from(ex)],
    ///     store.named_graphs().await?
    /// );
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn named_graphs(&self) -> Result<Vec<NamedOrBlankNode>, StorageError> {
        self.engine.storage().named_graphs().await
    }

    /// Checks if the store contains a given graph
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::{NamedNode, QuadRef};
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNode::new("http://example.com")?;
    /// let store = Store::new();
    /// store.insert(QuadRef::new(&ex, &ex, &ex, &ex)).await?;
    /// assert!(store.contains_named_graph(&ex).await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn contains_named_graph<'a>(
        &self,
        graph_name: impl Into<NamedOrBlankNodeRef<'a>>,
    ) -> Result<bool, QueryEvaluationError> {
        self.engine
            .storage()
            .contains_named_graph(graph_name.into())
            .await
            .map_err(Into::into)
    }

    /// Inserts a graph into this store.
    ///
    /// Returns `true` if the graph was not already in the store.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::NamedNodeRef;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let store = Store::new();
    /// store.insert_named_graph(ex).await?;
    ///
    /// assert_eq!(
    ///     store.named_graphs().await?,
    ///     vec![ex.into_owned().into()]
    /// );
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn insert_named_graph<'a>(
        &self,
        graph_name: impl Into<NamedOrBlankNodeRef<'a>>,
    ) -> Result<bool, StorageError> {
        self.engine
            .storage()
            .insert_named_graph(graph_name.into())
            .await
    }

    /// Clears a graph from this store.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::{NamedNodeRef, QuadRef};
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let quad = QuadRef::new(ex, ex, ex, ex);
    /// let store = Store::new();
    /// store.insert(quad).await?;
    /// assert_eq!(1, store.len().await?);
    ///
    /// store.clear_graph(ex).await?;
    /// assert!(store.is_empty().await?);
    /// assert_eq!(1, store.named_graphs().await?.len());
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn clear_graph<'a>(
        &self,
        graph_name: impl Into<GraphNameRef<'a>>,
    ) -> Result<(), StorageError> {
        self.engine.storage().clear_graph(graph_name.into()).await
    }

    /// Removes a graph from this store.
    ///
    /// Returns `true` if the graph was in the store and has been removed.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::{NamedNodeRef, QuadRef};
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let quad = QuadRef::new(ex, ex, ex, ex);
    /// let store = Store::new();
    /// store.insert(quad).await?;
    /// assert_eq!(1, store.len().await?);
    ///
    /// assert!(store.remove_named_graph(ex).await?);
    /// assert!(store.is_empty().await?);
    /// assert_eq!(0, store.named_graphs().await?.len());
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn remove_named_graph<'a>(
        &self,
        graph_name: impl Into<NamedOrBlankNodeRef<'a>>,
    ) -> Result<bool, StorageError> {
        self.engine
            .storage()
            .remove_named_graph(graph_name.into())
            .await
    }

    /// Clears the store.
    ///
    /// Usage example:
    /// ```
    /// use rdf_fusion::model::*;
    /// use rdf_fusion::store::Store;
    ///
    /// # tokio_test::block_on(async {
    /// let ex = NamedNodeRef::new("http://example.com")?;
    /// let store = Store::new();
    /// store.insert(QuadRef::new(ex, ex, ex, ex)).await?;
    /// store.insert(QuadRef::new(ex, ex, ex, GraphNameRef::DefaultGraph)).await?;
    /// assert_eq!(2, store.len().await?);
    ///
    /// store.clear().await?;
    /// assert!(store.is_empty().await?);
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// # }).unwrap();
    /// ```
    pub async fn clear(&self) -> Result<(), StorageError> {
        self.engine.storage().clear().await
    }

    /// Validates that all the store invariants held in the data
    #[allow(clippy::unused_self, reason = "Not implemented")]
    #[allow(clippy::unnecessary_wraps, reason = "Not implemented")]
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
    use rdf_fusion_model::{BlankNode, GraphName, Literal, NamedNode, Subject, Term};

    #[test]
    fn test_send_sync() {
        fn is_send_sync<T: Send + Sync>() {}
        is_send_sync::<Store>();
    }

    #[tokio::test]
    async fn store() -> Result<(), QueryEvaluationError> {
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

        let store = Store::new();
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
        assert_eq!(store.stream().await?.try_collect_to_vec().await?, all_quads);
        assert_eq!(
            store
                .quads_for_pattern(Some(main_s.as_ref()), None, None, None)
                .await?
                .try_collect_to_vec()
                .await?,
            all_quads
        );
        assert_eq!(
            store
                .quads_for_pattern(Some(main_s.as_ref()), Some(main_p.as_ref()), None, None)
                .await?
                .try_collect_to_vec()
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
                .try_collect_to_vec()
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
                .try_collect_to_vec()
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
                .try_collect_to_vec()
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
                .try_collect_to_vec()
                .await?,
            default_quads
        );
        assert_eq!(
            store
                .quads_for_pattern(Some(main_s.as_ref()), None, Some(main_o.as_ref()), None)
                .await?
                .try_collect_to_vec()
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
                .try_collect_to_vec()
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
                .try_collect_to_vec()
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
                .try_collect_to_vec()
                .await?,
            default_quads
        );
        assert_eq!(
            store
                .quads_for_pattern(None, Some(main_p.as_ref()), None, None)
                .await?
                .try_collect_to_vec()
                .await?,
            all_quads
        );
        assert_eq!(
            store
                .quads_for_pattern(None, Some(main_p.as_ref()), Some(main_o.as_ref()), None)
                .await?
                .try_collect_to_vec()
                .await?,
            vec![named_quad.clone(), default_quad.clone()]
        );
        assert_eq!(
            store
                .quads_for_pattern(None, None, Some(main_o.as_ref()), None)
                .await?
                .try_collect_to_vec()
                .await?,
            vec![named_quad.clone(), default_quad.clone()]
        );
        assert_eq!(
            store
                .quads_for_pattern(None, None, None, Some(GraphNameRef::DefaultGraph))
                .await?
                .try_collect_to_vec()
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
                .try_collect_to_vec()
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
                .try_collect_to_vec()
                .await?,
            vec![named_quad]
        );

        Ok(())
    }
}
