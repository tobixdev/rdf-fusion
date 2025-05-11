use crate::planner::GraphFusionPlanner;
use crate::sparql::error::QueryEvaluationError;
use crate::sparql::{
    evaluate_query, Query, QueryExplanation, QueryOptions, QueryResults, Variable,
};
use crate::{DFResult, QuadStorage};
use datafusion::dataframe::DataFrame;
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder};
use datafusion::functions_aggregate::first_last::FirstValue;
use datafusion::logical_expr::{AggregateUDF, Extension, LogicalPlan};
use datafusion::prelude::SessionContext;
use graphfusion_encoding::TABLE_QUADS;
use graphfusion_functions::registry::{
    GraphFusionFunctionRegistry, GraphFusionFunctionRegistryRef,
};
use graphfusion_logical::patterns::QuadPatternNode;
use graphfusion_model::{GraphNameRef, NamedNodeRef, QuadRef, SubjectRef, TermRef};
use spargebra::term::{BlankNode, GraphNamePattern, NamedNodePattern, QuadPattern, TermPattern};
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
    /// Holds references to the registered built-in functions.
    functions: GraphFusionFunctionRegistryRef,
    /// The storage that backs this instance.
    storage: Arc<dyn QuadStorage>,
}

impl GraphFusionInstance {
    /// Creates a new [GraphFusionInstance] with the default configuration and the given `storage`.
    pub fn new_with_storage(storage: Arc<dyn QuadStorage>) -> DFResult<Self> {
        // TODO make a builder

        let builtins = Arc::new(GraphFusionFunctionRegistry::default());

        let state = SessionStateBuilder::new()
            .with_query_planner(Arc::new(GraphFusionPlanner))
            .with_aggregate_functions(vec![AggregateUDF::from(FirstValue::new()).into()])
            // .with_optimizer_rule(Arc::new(PathToJoinsRule::new(
            //     Arc::clone(&builtins),
            //     storage.table_provider(),
            // )))
            // .with_optimizer_rule(Arc::new(PatternToProjectionRule::new(Arc::clone(
            //     &builtins,
            // ))))
            .build();

        let session_context = SessionContext::from(state);
        session_context.register_table("quads", storage.table_provider())?;

        Ok(Self {
            ctx: session_context,
            functions: builtins,
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
            &self.functions,
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
        let plan =
            create_match_pattern_plan(&self.functions, graph_name, subject, predicate, object)?;

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
        evaluate_query(&self.ctx, Arc::clone(&self.functions), query, options).await
    }
}

/// Creates a [LogicalPlan] for computing all quads that match the given pattern.
fn create_match_pattern_plan(
    registry: &GraphFusionFunctionRegistryRef,
    graph_name: Option<GraphNameRef>,
    subject: Option<SubjectRef>,
    predicate: Option<NamedNodeRef>,
    object: Option<TermRef>,
) -> DFResult<LogicalPlan> {
    let graph_name = graph_name
        .map(|g| match g {
            GraphNameRef::NamedNode(nn) => GraphNamePattern::NamedNode(nn.into_owned()),
            GraphNameRef::BlankNode(bnode) => {
                GraphNamePattern::Variable(Variable::new_unchecked(bnode.as_str()))
            }
            GraphNameRef::DefaultGraph => GraphNamePattern::DefaultGraph,
        })
        .unwrap_or(GraphNamePattern::Variable(Variable::new_unchecked(
            BlankNode::default().as_str(),
        )));

    let subject = subject
        .map(|s| TermPattern::from(s.into_owned()))
        .unwrap_or(TermPattern::BlankNode(BlankNode::default()));
    let predicate = predicate
        .map(|p| NamedNodePattern::from(p.into_owned()))
        .unwrap_or(NamedNodePattern::Variable(Variable::new_unchecked(
            BlankNode::default().as_str(),
        )));
    let object = object
        .map(|o| TermPattern::from(o.into_owned()))
        .unwrap_or(TermPattern::BlankNode(BlankNode::default()));
    let quad_pattern = QuadPattern {
        subject,
        predicate,
        object,
        graph_name,
    };

    let plan = QuadPatternNode::new(quad_pattern);
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(plan),
    }))
}
