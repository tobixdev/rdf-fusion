use crate::planner::RdfFusionPlanner;
use crate::sparql::error::QueryEvaluationError;
use crate::sparql::{evaluate_query, Query, QueryExplanation, QueryOptions, QueryResults};
use crate::{DFResult, QuadStorage};
use datafusion::dataframe::DataFrame;
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder};
use datafusion::functions_aggregate::first_last::FirstValue;
use datafusion::logical_expr::AggregateUDF;
use datafusion::prelude::SessionContext;
use rdf_fusion_encoding::TABLE_QUADS;
use rdf_fusion_functions::registry::{
    DefaultRdfFusionFunctionRegistry, RdfFusionFunctionRegistry, RdfFusionFunctionRegistryRef,
};
use rdf_fusion_logical::extend::ExtendLoweringRule;
use rdf_fusion_logical::join::SparqlJoinLoweringRule;
use rdf_fusion_logical::minus::MinusLoweringRule;
use rdf_fusion_logical::paths::PropertyPathLoweringRule;
use rdf_fusion_logical::patterns::PatternLoweringRule;
use rdf_fusion_logical::quads::QuadsLoweringRule;
use rdf_fusion_logical::{ActiveGraph, RdfFusionLogicalPlanBuilder};
use rdf_fusion_model::{GraphName, GraphNameRef, NamedNodeRef, QuadRef, SubjectRef, TermRef};
use std::sync::Arc;

/// Represents an instance of a RdfFusion engine.
///
/// A RdfFusion instance consists of:
/// - A [SessionContext]. This is the primary interaction point with DataFusion.
/// - A reference to a storage backend that holds quads. This reference is used for updates via the
///   store API.
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
            Arc::new(DefaultRdfFusionFunctionRegistry::default());

        let state = SessionStateBuilder::new()
            .with_query_planner(Arc::new(RdfFusionPlanner))
            .with_aggregate_functions(vec![AggregateUDF::from(FirstValue::new()).into()])
            .with_optimizer_rule(Arc::new(MinusLoweringRule::new(Arc::clone(&registry))))
            .with_optimizer_rule(Arc::new(ExtendLoweringRule::new()))
            .with_optimizer_rule(Arc::new(PropertyPathLoweringRule::new(Arc::clone(
                &registry,
            ))))
            .with_optimizer_rule(Arc::new(SparqlJoinLoweringRule::new(Arc::clone(&registry))))
            .with_optimizer_rule(Arc::new(PatternLoweringRule::new(Arc::clone(&registry))))
            .with_optimizer_rule(Arc::new(QuadsLoweringRule::new(
                Arc::clone(&registry),
                storage.table_provider(),
            )))
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
        let pattern_plan = RdfFusionLogicalPlanBuilder::new_from_quads(
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
        let pattern_plan = RdfFusionLogicalPlanBuilder::new_from_quads(
            Arc::clone(&self.functions),
            active_graph_info,
            subject.map(SubjectRef::into_owned),
            predicate.map(NamedNodeRef::into_owned),
            object.map(TermRef::into_owned),
        );

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
    ) -> Result<(QueryResults, Option<QueryExplanation>), QueryEvaluationError> {
        evaluate_query(&self.ctx, Arc::clone(&self.functions), query, options).await
    }
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
