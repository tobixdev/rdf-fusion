use crate::patterns::pattern_element::PatternNodeElement;
use crate::patterns::PatternNode;
use crate::DFResult;
use arrow_rdf::value_encoding::{ENC_EFFECTIVE_BOOLEAN_VALUE, ENC_SAME_TERM};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::DFSchema;
use datafusion::logical_expr::{and, col, Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::Expr;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default)]
pub struct PatternToProjectionRule;

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

                        let filter = compute_filters_for_pattern(node);
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
pub fn compute_filters_for_pattern(node: &PatternNode) -> Option<Expr> {
    let filters = [
        filter_by_values(node.input().schema(), node.patterns()),
        filter_same_variable(node.input().schema(), node.patterns()),
    ];
    filters.into_iter().flatten().reduce(and)
}

/// Adds filter operations that constraints the solutions of patterns that use literals.
///
/// For example, for the pattern `?a foaf:knows ?b` this functions adds a filter that ensures that
/// the predicate is `foaf:knows`.
fn filter_by_values(schema: &DFSchema, pattern: &[PatternNodeElement]) -> Option<Expr> {
    schema
        .columns()
        .iter()
        .zip(pattern.iter())
        .filter_map(|(c, p)| p.filter_expression(c))
        .reduce(and)
}

/// Adds filter operations that constraints the solutions of patterns that use the same variable
/// twice.
///
/// For example, for the pattern `?a ?a ?b` this functions adds a constraint that ensures that the
/// subject is equal to the predicate.
fn filter_same_variable(schema: &DFSchema, pattern: &[PatternNodeElement]) -> Option<Expr> {
    let mut mappings = HashMap::new();

    let column_patterns = schema.columns().into_iter().zip(pattern.iter());
    for (column, pattern) in column_patterns {
        if let Some(variable) = pattern.variable_name() {
            if !mappings.contains_key(&variable) {
                mappings.insert(variable.clone(), Vec::new());
            }
            mappings.get_mut(&variable).unwrap().push(column.clone());
        }
    }

    let mut constraints = Vec::new();
    for value in mappings.into_values() {
        let columns = value.into_iter().map(col).collect::<Vec<_>>();
        let constraint = columns
            .iter()
            .zip(columns.iter().skip(1))
            .map(|(a, b)| {
                ENC_EFFECTIVE_BOOLEAN_VALUE
                    .call(vec![ENC_SAME_TERM.call(vec![a.clone(), b.clone()])])
            })
            .reduce(Expr::and);
        if let Some(constraint) = constraint {
            constraints.push(constraint);
        }
    }

    constraints.into_iter().reduce(Expr::and)
}

/// Projects the inner columns to the variables.
fn project_to_variables(
    plan: LogicalPlanBuilder,
    patterns: &[PatternNodeElement],
) -> DFResult<LogicalPlanBuilder> {
    let possible_projections = plan
        .schema()
        .columns()
        .into_iter()
        .zip(patterns.iter())
        .filter_map(|(c, p)| p.variable_name().map(|vname| (c, vname)));

    let mut already_projected = HashSet::new();
    let mut projections = Vec::new();
    for (old_name, new_name) in possible_projections {
        if !already_projected.contains(&new_name) {
            already_projected.insert(new_name.clone());

            let expr = Expr::from(old_name.clone()).alias(new_name);
            projections.push(expr);
        }
    }

    plan.project(projections)
}
