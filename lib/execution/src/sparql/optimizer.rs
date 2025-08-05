use crate::sparql::OptimizationLevel;
use datafusion::optimizer::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
use datafusion::optimizer::eliminate_limit::EliminateLimit;
use datafusion::optimizer::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
use datafusion::optimizer::scalar_subquery_to_join::ScalarSubqueryToJoin;
use datafusion::optimizer::{Optimizer, OptimizerRule};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use rdf_fusion_api::RdfFusionContextView;
use rdf_fusion_logical::expr::SimplifySparqlExpressionsRule;
use rdf_fusion_logical::extend::ExtendLoweringRule;
use rdf_fusion_logical::join::{SparqlJoinLoweringRule, SparqlJoinReorderingRule};
use rdf_fusion_logical::minus::MinusLoweringRule;
use rdf_fusion_logical::paths::PropertyPathLoweringRule;
use rdf_fusion_logical::patterns::PatternLoweringRule;
use rdf_fusion_physical::join::NestedLoopJoinProjectionPushDown;
use std::sync::Arc;

/// Creates a list of optimizer rules based on the given `optimization_level`.
pub fn create_optimizer_rules(
    context: RdfFusionContextView,
    optimization_level: OptimizationLevel,
) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let lowering_rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![
        Arc::new(MinusLoweringRule::new(context.clone())),
        Arc::new(ExtendLoweringRule::new()),
        Arc::new(PropertyPathLoweringRule::new(context.clone())),
        Arc::new(SparqlJoinLoweringRule::new(context.clone())),
        Arc::new(PatternLoweringRule::new(context.clone())),
    ];

    match optimization_level {
        OptimizationLevel::None => {
            let mut rules = Vec::new();
            rules.extend(lowering_rules);
            rules.extend(create_essential_datafusion_optimizers());
            rules
        }
        OptimizationLevel::Default => {
            let mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = Vec::new();
            rules.push(Arc::new(SparqlJoinReorderingRule::new(
                context.encodings().clone(),
            )));
            rules.extend(lowering_rules);

            // DataFusion Optimizers
            // TODO: Replace with a good subset
            rules.extend(create_essential_datafusion_optimizers());

            rules.push(Arc::new(SimplifySparqlExpressionsRule::new(
                context.clone(),
            )));
            rules
        }
        OptimizationLevel::Full => {
            let mut rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = Vec::new();
            rules.push(Arc::new(SparqlJoinReorderingRule::new(
                context.encodings().clone(),
            )));
            rules.extend(lowering_rules);
            rules.extend(Optimizer::default().rules);
            rules.push(Arc::new(SimplifySparqlExpressionsRule::new(
                context.clone(),
            )));
            rules
        }
    }
}

fn create_essential_datafusion_optimizers() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    vec![
        Arc::new(ReplaceDistinctWithAggregate::new()),
        Arc::new(DecorrelatePredicateSubquery::new()),
        Arc::new(EliminateLimit::new()),
        Arc::new(ScalarSubqueryToJoin::new()),
    ]
}

/// Creates a list of optimizer rules based on the given `optimization_level`.
pub fn create_pyhsical_optimizer_rules(
    _optimization_level: OptimizationLevel,
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    // TODO: build based on optimization level
    let mut rules = PhysicalOptimizer::default().rules;
    rules.push(Arc::new(NestedLoopJoinProjectionPushDown::new()));
    rules
}
