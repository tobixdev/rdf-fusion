use crate::planner::RdfFusionPlanner;
use crate::sparql::error::QueryEvaluationError;
use crate::sparql::{evaluate_query, Query, QueryExplanation, QueryOptions, QueryResults};
use datafusion::dataframe::DataFrame;
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder};
use datafusion::functions_aggregate::first_last::FirstValue;
use datafusion::logical_expr::AggregateUDF;
use datafusion::optimizer::{Optimizer, OptimizerRule};
use datafusion::prelude::{SessionConfig, SessionContext};
use rdf_fusion_common::{DFResult, QuadStorage};
use rdf_fusion_encoding::TABLE_QUADS;
use rdf_fusion_functions::registry::{
    DefaultRdfFusionFunctionRegistry, RdfFusionFunctionRegistry, RdfFusionFunctionRegistryRef,
};
use rdf_fusion_logical::expr::SimplifySparqlExpressionsRule;
use rdf_fusion_logical::extend::ExtendLoweringRule;
use rdf_fusion_logical::join::{SparqlJoinLoweringRule, SparqlJoinReorderingRule};
use rdf_fusion_logical::minus::MinusLoweringRule;
use rdf_fusion_logical::paths::PropertyPathLoweringRule;
use rdf_fusion_logical::patterns::PatternLoweringRule;
use rdf_fusion_logical::{ActiveGraph, RdfFusionLogicalPlanBuilder};
use rdf_fusion_model::{GraphName, GraphNameRef, NamedNodeRef, QuadRef, SubjectRef, TermRef};
use std::sync::Arc;

/// Represents an instance of an RDF Fusion engine.
///
/// An RDF Fusion instance consists of:
/// - A [SessionContext]. This is the primary interaction point with DataFusion.
/// - An [RdfFusionFunctionRegistry] that holds the currently registered RDF Fusion built-ins.
/// - A reference to a quad storage.
#[derive(Clone)]
pub struct RdfFusionInstance {
    /// The DataFusion [SessionContext].
    ctx: SessionContext,
    /// Holds references to the registered built-in functions.
    functions: RdfFusionFunctionRegistryRef,
    /// The storage that backs this instance.
    storage: Arc<dyn QuadStorage>,
}

impl RdfFusionInstance {
    /// Creates a new [RdfFusionInstance] with the default configuration and the given `storage`.
    pub fn new_with_storage(storage: Arc<dyn QuadStorage>) -> DFResult<Self> {
        // TODO make a builder

        let registry: Arc<dyn RdfFusionFunctionRegistry> =
            Arc::new(DefaultRdfFusionFunctionRegistry);

        let state = SessionStateBuilder::new()
            .with_query_planner(Arc::new(RdfFusionPlanner::new(Arc::clone(&storage))))
            .with_aggregate_functions(vec![AggregateUDF::from(FirstValue::new()).into()])
            .with_optimizer_rules(create_default_optimizer_rules(&registry))
            // TODO: For now we use only a single partition. This should be configurable.
            .with_config(SessionConfig::new().with_target_partitions(1))
            .build();

        let session_context = SessionContext::from(state);
        session_context.register_table("quads", storage.table_provider())?;

        Ok(Self {
            ctx: session_context,
            functions: registry,
            storage,
        })
    }

    /// Provides access to the [QuadStorage] of this instance for writing operations.
    pub fn storage(&self) -> &dyn QuadStorage {
        self.storage.as_ref()
    }

    //
    // Querying
    //

    /// Checks whether `quad` is contained in the instance.
    pub async fn contains(&self, quad: &QuadRef<'_>) -> DFResult<bool> {
        let active_graph_info = graph_name_to_active_graph(Some(quad.graph_name));
        let pattern_plan = RdfFusionLogicalPlanBuilder::new_from_matching_quads(
            Arc::clone(&self.functions),
            active_graph_info,
            Some(quad.subject.into_owned()),
            Some(quad.predicate.into_owned()),
            Some(quad.object.into_owned()),
        );

        let count = DataFrame::new(self.ctx.state(), pattern_plan.build()?)
            .count()
            .await?;

        Ok(count > 0)
    }

    /// Returns the number of quads in the instance.
    pub async fn len(&self) -> DFResult<usize> {
        self.ctx.table(TABLE_QUADS).await?.count().await
    }

    /// Returns a stream of all quads that match the given pattern.
    pub async fn quads_for_pattern(
        &self,
        graph_name: Option<GraphNameRef<'_>>,
        subject: Option<SubjectRef<'_>>,
        predicate: Option<NamedNodeRef<'_>>,
        object: Option<TermRef<'_>>,
    ) -> DFResult<SendableRecordBatchStream> {
        let active_graph_info = graph_name_to_active_graph(graph_name);
        let pattern_plan = RdfFusionLogicalPlanBuilder::new_from_matching_quads(
            Arc::clone(&self.functions),
            active_graph_info,
            subject.map(SubjectRef::into_owned),
            predicate.map(NamedNodeRef::into_owned),
            object.map(TermRef::into_owned),
        )
        .with_plain_terms()?;

        let result = DataFrame::new(self.ctx.state(), pattern_plan.build()?)
            .execute_stream()
            .await?;
        Ok(result)
    }

    /// Evaluates a SPARQL [Query] over the instance.
    pub async fn execute_query(
        &self,
        query: &Query,
        options: QueryOptions,
    ) -> Result<(QueryResults, QueryExplanation), QueryEvaluationError> {
        evaluate_query(&self.ctx, Arc::clone(&self.functions), query, options).await
    }
}

fn create_default_optimizer_rules(
    registry: &RdfFusionFunctionRegistryRef,
) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
        Arc::new(SparqlJoinReorderingRule::new()),
        Arc::new(MinusLoweringRule::new(Arc::clone(registry))),
        Arc::new(ExtendLoweringRule::new()),
        Arc::new(PropertyPathLoweringRule::new(Arc::clone(registry))),
        Arc::new(SparqlJoinLoweringRule::new(Arc::clone(registry))),
        Arc::new(PatternLoweringRule::new(Arc::clone(registry))),
    ];

    rules.extend(Optimizer::default().rules);

    rules.push(Arc::new(SimplifySparqlExpressionsRule::new()));

    rules
}

fn graph_name_to_active_graph(graph_name: Option<GraphNameRef<'_>>) -> ActiveGraph {
    let Some(graph_name) = graph_name else {
        return ActiveGraph::AllGraphs;
    };

    match graph_name {
        GraphNameRef::NamedNode(nn) => {
            ActiveGraph::Union(vec![GraphName::NamedNode(nn.into_owned())])
        }
        GraphNameRef::BlankNode(bnode) => {
            ActiveGraph::Union(vec![GraphName::BlankNode(bnode.into_owned())])
        }
        GraphNameRef::DefaultGraph => ActiveGraph::DefaultGraph,
    }
}
