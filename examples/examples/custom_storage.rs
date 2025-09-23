use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::{DFSchema, HashSet};
use datafusion::datasource::MemTable;
use datafusion::datasource::TableProvider;
use datafusion::execution::SessionState;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNode, col, lit};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::{SessionConfig, and};
use rdf_fusion::api::storage::QuadStorage;
use rdf_fusion::encoding::object_id::ObjectIdMapping;
use rdf_fusion::encoding::plain_term::{
    PLAIN_TERM_ENCODING, PlainTermArrayElementBuilder, PlainTermEncoding,
};
use rdf_fusion::encoding::{EncodingArray, EncodingScalar, QuadStorageEncoding};
use rdf_fusion::execution::RdfFusionContext;
use rdf_fusion::execution::results::QueryResultsFormat;
use rdf_fusion::logical::ActiveGraph;
use rdf_fusion::logical::quad_pattern::QuadPatternNode;
use rdf_fusion::model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion::model::{
    GraphName, GraphNameRef, NamedNode, NamedOrBlankNode, NamedOrBlankNodeRef, Quad,
    QuadRef, StorageError, TermPattern, Variable,
};
use rdf_fusion::store::Store;
use std::sync::Arc;

/// This example shows how to use a custom storage layer for RDF Fusion.
#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let vec = HashSet::from([
        Quad::new(
            NamedNode::new("http://example.org/#spiderman")?,
            NamedNode::new("http://www.perceive.net/schemas/relationship/enemyOf")?,
            NamedNode::new("http://example.org/#green-goblin")?,
            GraphName::DefaultGraph,
        ),
        Quad::new(
            NamedNode::new("http://example.org/#spiderman")?,
            NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")?,
            NamedNode::new("http://xmlns.com/foaf/0.1/Person")?,
            GraphName::DefaultGraph,
        ),
    ]);

    let context = RdfFusionContext::new(
        SessionConfig::default(),
        RuntimeEnvBuilder::new().build_arc()?,
        Arc::new(VecQuadStorage(vec)),
    );
    let store = Store::new(context);

    // Run SPARQL query.
    let query = "
    BASE <http://example.org/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT ?person
    WHERE {
        ?person a foaf:Person .
    }
    ";
    let result = store.query(query).await?;

    // Serialize result
    let mut result_buffer = Vec::new();
    result
        .write(&mut result_buffer, QueryResultsFormat::Csv)
        .await?;
    let result = String::from_utf8(result_buffer)?;

    // Print results.
    println!("Persons:");
    print!("{result}");

    Ok(())
}

/// This is the custom storage layer that we use for this example.
///
/// The database is a simple set of quads that cannot be changed after creating the storage (for
/// the sake of simplicity).
struct VecQuadStorage(HashSet<Quad>);

impl VecQuadStorage {
    /// Creates a [MemTable] for the set. This is a struct from DataFusion that simply emits
    /// references to record batches.
    pub fn create_mem_table(&self) -> MemTable {
        let mut graph_name = PlainTermArrayElementBuilder::new(self.0.len());
        let mut subject = PlainTermArrayElementBuilder::new(self.0.len());
        let mut predicate = PlainTermArrayElementBuilder::new(self.0.len());
        let mut object = PlainTermArrayElementBuilder::new(self.0.len());

        for quad in &self.0 {
            match &quad.graph_name {
                GraphName::NamedNode(node) => {
                    graph_name.append_term(node.as_ref().into())
                }
                GraphName::BlankNode(node) => {
                    graph_name.append_term(node.as_ref().into())
                }
                GraphName::DefaultGraph => graph_name.append_null(),
            }
            subject.append_term(quad.subject.as_ref().into());
            predicate.append_term(quad.predicate.as_ref().into());
            object.append_term(quad.object.as_ref().into());
        }

        let graph_name = graph_name.finish();
        let subject = subject.finish();
        let predicate = predicate.finish();
        let object = object.finish();

        let schema = SchemaRef::new(Schema::new(vec![
            Field::new(COL_GRAPH, PlainTermEncoding::data_type(), true),
            Field::new(COL_SUBJECT, PlainTermEncoding::data_type(), false),
            Field::new(COL_PREDICATE, PlainTermEncoding::data_type(), false),
            Field::new(COL_OBJECT, PlainTermEncoding::data_type(), false),
        ]));

        let record_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                graph_name.into_array(),
                subject.into_array(),
                predicate.into_array(),
                object.into_array(),
            ],
        )
        .expect("Schema and length always match");

        MemTable::try_new(Arc::clone(&schema), vec![vec![record_batch]])
            .expect("Schemas always match")
    }
}

#[async_trait]
impl QuadStorage for VecQuadStorage {
    fn encoding(&self) -> QuadStorageEncoding {
        // We use the plain term encoding for the quads.
        QuadStorageEncoding::PlainTerm
    }

    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdMapping>> {
        // We do not have an object ID mapping.
        None
    }

    async fn planners(&self) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
        // One important thing is that the storage layer is responsible for planning the quad nodes.
        // This is why we need to register a custom planner.
        let mem_table = self.create_mem_table();
        vec![Arc::new(VecQuadStoragePlanner(mem_table))]
    }

    async fn extend(&self, _quads: Vec<Quad>) -> Result<usize, StorageError> {
        unimplemented!("Mutating a VecQuadStorage is not supported")
    }

    async fn remove(&self, _quad: QuadRef<'_>) -> Result<bool, StorageError> {
        unimplemented!("Mutating a VecQuadStorage is not supported")
    }

    async fn insert_named_graph<'a>(
        &self,
        _graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        unimplemented!("Mutating a VecQuadStorage is not supported")
    }

    async fn named_graphs(&self) -> Result<Vec<NamedOrBlankNode>, StorageError> {
        unimplemented!("Mutating a VecQuadStorage is not supported")
    }

    async fn contains_named_graph<'a>(
        &self,
        _graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        unimplemented!("Mutating a VecQuadStorage is not supported")
    }

    async fn clear(&self) -> Result<(), StorageError> {
        unimplemented!("Mutating a VecQuadStorage is not supported")
    }

    async fn clear_graph<'a>(
        &self,
        _graph_name: GraphNameRef<'a>,
    ) -> Result<(), StorageError> {
        unimplemented!("Mutating a VecQuadStorage is not supported")
    }

    async fn drop_named_graph(
        &self,
        _graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<bool, StorageError> {
        unimplemented!("Mutating a VecQuadStorage is not supported")
    }

    async fn len(&self) -> Result<usize, StorageError> {
        Ok(self.0.len())
    }

    async fn optimize(&self) -> Result<(), StorageError> {
        Ok(())
    }

    async fn validate(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

/// A custom planner that plans the quad pattern nodes based on given [MemTable]. We assume that
/// the table has the following schema: (graph, subject, predicate, object).
///
/// Evaluating a pattern will be done in three steps:
/// 1. Full scan over the [MemTable].
/// 2. Apply filters (e.g., if an element is a constant `foaf:Person`)
/// 3. Apply projections (e.g., subject -> ?person)
///
/// Usually, implementation will tightly couple these three steps to improve performance.
struct VecQuadStoragePlanner(MemTable);

#[async_trait]
impl ExtensionPlanner for VecQuadStoragePlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
        // Only plan quad pattern nodes.
        let Some(node) = node.as_any().downcast_ref::<QuadPatternNode>() else {
            return Ok(None);
        };

        // 1. Full Scan
        let scan = self.0.scan(session_state, None, &[], None).await?;
        let dfschema = DFSchema::try_from(scan.schema())?;

        // 2. Apply filters
        let filter_expr = create_filter(&node)?
            .map(|e| planner.create_physical_expr(&e, &dfschema, session_state))
            .transpose()?;
        let plan = match filter_expr {
            None => scan,
            Some(filter) => Arc::new(FilterExec::try_new(filter, scan)?),
        };

        // 3. Apply projections
        let projections = create_projections(&node)
            .into_iter()
            .map(|(var, column_name)| {
                let expr = planner.create_physical_expr(
                    &col(column_name),
                    &dfschema,
                    session_state,
                )?;
                Ok::<ProjectionExpr, datafusion::common::DataFusionError>(
                    ProjectionExpr::new(expr, var.to_string()),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let projection = ProjectionExec::try_new(projections, plan)?;

        Ok(Some(Arc::new(projection)))
    }
}

/// Create the filters stemming from the pattern.
fn create_filter(node: &QuadPatternNode) -> datafusion::common::Result<Option<Expr>> {
    let mut exprs = Vec::new();

    match node.active_graph() {
        ActiveGraph::DefaultGraph => {
            exprs.push(col(COL_GRAPH).is_null());
        }
        _ => unimplemented!("Currently, only the default graph is supported"),
    }

    let predicate = TermPattern::from(node.pattern().predicate.clone());
    let patterns = [
        (&node.pattern().subject, COL_SUBJECT),
        (&predicate, COL_PREDICATE),
        (&node.pattern().object, COL_OBJECT),
    ];

    for (pattern, col_name) in patterns {
        match pattern {
            TermPattern::NamedNode(nn) => {
                let scalar = PLAIN_TERM_ENCODING.encode_term(Ok(nn.as_ref().into()))?;
                exprs.push(col(col_name).eq(lit(scalar.into_scalar_value())));
            }
            TermPattern::BlankNode(node) => {
                let scalar = PLAIN_TERM_ENCODING.encode_term(Ok(node.as_ref().into()))?;
                exprs.push(col(col_name).eq(lit(scalar.into_scalar_value())));
            }
            TermPattern::Literal(node) => {
                let scalar = PLAIN_TERM_ENCODING.encode_term(Ok(node.as_ref().into()))?;
                exprs.push(col(col_name).eq(lit(scalar.into_scalar_value())));
            }
            TermPattern::Variable(_) => {}
        }
    }

    // Actually, we would also need to check if two elements have the same variable :)

    Ok(exprs.into_iter().reduce(|a, b| and(a, b)))
}

/// Creates the projections for the pattern.
fn create_projections(node: &QuadPatternNode) -> Vec<(String, String)> {
    let mut projections = Vec::new();

    let pairs = [
        (node.graph_variable().map(|v| v.into_owned()), COL_GRAPH),
        (var(node.pattern().subject.clone()), COL_SUBJECT),
        (var(node.pattern().predicate.clone()), COL_PREDICATE),
        (var(node.pattern().object.clone()), COL_OBJECT),
    ];

    for (var, column_name) in pairs {
        if let Some(var) = var {
            projections.push((var.as_str().to_owned(), column_name.to_owned()))
        }
    }

    projections
}

/// Helper function to extract a variable from a pattern.
fn var(pattern: impl Into<TermPattern>) -> Option<Variable> {
    let pattern = pattern.into();
    match pattern {
        TermPattern::Variable(v) => Some(v),
        _ => None,
    }
}
