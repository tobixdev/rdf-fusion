use crate::planner::GraphFusionPlanner;
use crate::sparql::error::QueryEvaluationError;
use crate::sparql::{evaluate_query, Query, QueryExplanation, QueryOptions, QueryResults};
use crate::{DFResult, QuadStorage};
use graphfusion_encoding::TABLE_QUADS;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::DefaultTableSource;
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder};
use datafusion::functions_aggregate::first_last::FirstValue;
use datafusion::logical_expr::{AggregateUDF, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::SessionContext;
use graphfusion_logical::paths::PathToJoinsRule;
use graphfusion_logical::patterns::{
    compute_filters_for_pattern, PatternNode, PatternNodeElement, PatternToProjectionRule,
};
use model::{GraphNameRef, NamedNodeRef, QuadRef, SubjectRef, TermRef};
use spargebra::term::TermPattern;
use std::sync::Arc;

/// Represents an instance of a GraphFusion engine.
///
/// A GraphFusion instance consists of:
/// - A [SessionContext]. This is the primary interaction point with DataFusion.
/// - A reference to a storage backend that holds quads. This reference is used for updates via the
///   store API.
#[derive(Clone)]
pub struct GraphFusionInstance {
    /// The DataFusion [SessionContext].
    ctx: SessionContext,
    /// The storage that backs this instance.
    storage: Arc<dyn QuadStorage>,
}

impl GraphFusionInstance {
    /// Creates a new [GraphFusionInstance] with the default configuration and the given `storage`.
    pub fn new_with_storage(storage: Arc<dyn QuadStorage>) -> DFResult<Self> {
        let state = SessionStateBuilder::new()
            .with_query_planner(Arc::new(GraphFusionPlanner))
            .with_aggregate_functions(vec![AggregateUDF::from(FirstValue::new()).into()])
            .with_optimizer_rule(Arc::new(PathToJoinsRule::new(storage.table_provider())))
            .with_optimizer_rule(Arc::new(PatternToProjectionRule))
            .build();

        let session_context = SessionContext::from(state);
        session_context.register_table("quads", storage.table_provider())?;

        Ok(Self {
            ctx: session_context,
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
        let pattern_plan = create_match_pattern_plan(
            self.storage.as_ref(),
            Some(quad.graph_name),
            Some(quad.subject),
            Some(quad.predicate),
            Some(quad.object),
        )?;

        let count = DataFrame::new(self.ctx.state(), pattern_plan)
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
        let plan = create_match_pattern_plan(
            self.storage.as_ref(),
            graph_name,
            subject,
            predicate,
            object,
        )?;

        let result = DataFrame::new(self.ctx.state(), plan)
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
        evaluate_query(&self.ctx, query, options).await
    }
}

/// Creates a [LogicalPlan] for computing all quads that match the given pattern.
fn create_match_pattern_plan(
    storage: &dyn QuadStorage,
    graph_name: Option<GraphNameRef<'_>>,
    subject: Option<SubjectRef<'_>>,
    predicate: Option<NamedNodeRef<'_>>,
    object: Option<TermRef<'_>>,
) -> DFResult<LogicalPlan> {
    let plan = LogicalPlanBuilder::scan(
        TABLE_QUADS,
        Arc::new(DefaultTableSource::new(storage.table_provider())),
        None,
    )?;

    let graph_name = graph_name
        .map(|g| match g {
            GraphNameRef::NamedNode(nn) => PatternNodeElement::NamedNode(nn.into_owned()),
            GraphNameRef::BlankNode(bnode) => PatternNodeElement::BlankNode(bnode.into_owned()),
            GraphNameRef::DefaultGraph => PatternNodeElement::DefaultGraph,
        })
        .unwrap_or_default();
    let subject = subject
        .map(|s| TermPattern::from(s.into_owned()))
        .map(PatternNodeElement::from)
        .unwrap_or_default();
    let predicate = predicate
        .map(|p| TermPattern::from(p.into_owned()))
        .map(PatternNodeElement::from)
        .unwrap_or_default();
    let object = object
        .map(|o| TermPattern::from(o.into_owned()))
        .map(PatternNodeElement::from)
        .unwrap_or_default();
    let pattern_node = PatternNode::try_new(
        plan.clone().build()?,
        vec![graph_name, subject, predicate, object],
    )?;
    let filter = compute_filters_for_pattern(&pattern_node);

    let plan = match filter {
        None => plan.build()?,
        Some(filter) => plan.filter(filter)?.build()?,
    };
    Ok(plan)
}
