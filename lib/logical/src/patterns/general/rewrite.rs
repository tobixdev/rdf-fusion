use crate::expr_builder::GraphFusionExprBuilder;
use crate::patterns::PatternNode;
use crate::DFResult;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Column;
use datafusion::logical_expr::{and, col, Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::Expr;
use graphfusion_functions::registry::{
    GraphFusionFunctionRegistry, GraphFusionFunctionRegistryRef,
};
use spargebra::term::{Term, TermPattern};
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct PatternToProjectionRule {
    registry: GraphFusionFunctionRegistryRef,
}

impl PatternToProjectionRule {
    /// Creates a new [PatternToProjectionRule].
    pub fn new(registry: GraphFusionFunctionRegistryRef) -> Self {
        Self { registry }
    }
}

impl OptimizerRule for PatternToProjectionRule {
    fn name(&self) -> &str {
        "pattern_to_projection_rule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform(|plan| {
            let new_plan = match &plan {
                LogicalPlan::Extension(Extension { node }) => {
                    if let Some(node) = node.as_any().downcast_ref::<PatternNode>() {
                        let plan = LogicalPlanBuilder::from(node.input().clone());

                        let filter = compute_filters_for_pattern(&self.registry, node)?;
                        let plan = match filter {
                            None => plan,
                            Some(filter) => plan.filter(filter)?,
                        };
                        let plan = project_to_variables(plan, node.patterns())?;

                        Transformed::yes(plan.build()?)
                    } else {
                        Transformed::no(plan)
                    }
                }
                _ => Transformed::no(plan),
            };
            Ok(new_plan)
        })
    }
}

/// Computes the filters that will be applied for a given [PatternNode]. Callers can use this
/// function to only apply the filters of a pattern and ignore any projections to variables.
pub fn compute_filters_for_pattern(
    registry: &GraphFusionFunctionRegistry,
    node: &PatternNode,
) -> DFResult<Option<Expr>> {
    let expr_builder = GraphFusionExprBuilder::new(&node.input().schema(), registry);
    let filters = [
        filter_by_values(&expr_builder, node.patterns())?,
        filter_same_variable(&expr_builder, node.patterns())?,
    ];
    Ok(filters.into_iter().flatten().reduce(and))
}

/// Adds filter operations that constraints the solutions of patterns that use literals.
///
/// For example, for the pattern `?a foaf:knows ?b` this functions adds a filter that ensures that
/// the predicate is `foaf:knows`.
fn filter_by_values(
    expr_builder: &GraphFusionExprBuilder,
    pattern: &[Option<TermPattern>],
) -> DFResult<Option<Expr>> {
    let filters = expr_builder
        .schema()
        .columns()
        .iter()
        .zip(pattern.iter())
        .map(|(c, p)| create_filter_expression(&expr_builder, c, p.as_ref()))
        .collect::<DFResult<Vec<_>>>()?;
    Ok(filters.into_iter().filter_map(|f| f).reduce(and))
}

/// Adds filter operations that constraints the solutions of patterns that use the same variable
/// twice.
///
/// For example, for the pattern `?a ?a ?b` this functions adds a constraint that ensures that the
/// subject is equal to the predicate.
fn filter_same_variable(
    expr_builder: &GraphFusionExprBuilder,
    pattern: &[Option<TermPattern>],
) -> DFResult<Option<Expr>> {
    let mut mappings = HashMap::new();

    let column_patterns = expr_builder
        .schema()
        .columns()
        .into_iter()
        .zip(pattern.iter());
    for (column, pattern) in column_patterns {
        if let Some(TermPattern::Variable(variable)) = pattern {
            if !mappings.contains_key(variable) {
                mappings.insert(variable.clone(), Vec::new());
            }
            mappings.get_mut(&variable).unwrap().push(column.clone());
        }
    }

    let mut constraints = Vec::new();
    for value in mappings.into_values() {
        let columns = value.into_iter().map(col).collect::<Vec<_>>();
        let new_constraints = columns
            .iter()
            .zip(columns.iter().skip(1))
            .map(|(a, b)| expr_builder.same_term(a.clone(), b.clone()))
            .collect::<DFResult<Vec<_>>>()?;

        let mut new_constraint = new_constraints.into_iter().reduce(Expr::and);
        if let Some(constraint) = new_constraint {
            constraints.push(constraint);
        }
    }

    Ok(constraints.into_iter().reduce(Expr::and))
}

/// Projects the inner columns to the variables.
fn project_to_variables(
    plan: LogicalPlanBuilder,
    patterns: &[Option<TermPattern>],
) -> DFResult<LogicalPlanBuilder> {
    let possible_projections = plan
        .schema()
        .columns()
        .into_iter()
        .zip(patterns.iter())
        .filter_map(|(c, p)| match p {
            Some(TermPattern::Variable(v)) => Some((c, v)),
            _ => None,
        });

    let mut already_projected = HashSet::new();
    let mut projections = Vec::new();
    for (old_name, new_name) in possible_projections {
        if !already_projected.contains(new_name) {
            already_projected.insert(new_name.clone());

            let expr = Expr::from(old_name.clone()).alias(new_name.as_str());
            projections.push(expr);
        }
    }

    plan.project(projections)
}

/// Creates an [Expr] that filters `column` based on the contents of this element.
fn create_filter_expression(
    expr_builder: &GraphFusionExprBuilder,
    column: &Column,
    pattern: Option<&TermPattern>,
) -> DFResult<Option<Expr>> {
    let result = match pattern {
        Some(TermPattern::NamedNode(nn)) => Some(
            expr_builder.filter_by_scalar(col(column.clone()), Term::from(nn.clone()).as_ref())?,
        ),
        Some(TermPattern::Literal(lit)) => Some(
            expr_builder.filter_by_scalar(col(column.clone()), Term::from(lit.clone()).as_ref())?,
        ),
        _ => None,
    };
    Ok(result)
}
