use crate::patterns::{pattern_to_variable_name, PatternNode};
use crate::DFResult;
use arrow_rdf::encoded::scalars::{encode_scalar_literal, encode_scalar_named_node};
use arrow_rdf::encoded::{ENC_AS_NATIVE_BOOLEAN, ENC_EFFECTIVE_BOOLEAN_VALUE, ENC_SAME_TERM};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Column;
use datafusion::logical_expr::{and, col, lit, Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::Expr;
use spargebra::term::TermPattern;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default)]
pub struct PatternToProjectionRule {}

impl OptimizerRule for PatternToProjectionRule {
    fn name(&self) -> &str {
        "pattern_to_projection_rule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        self.rewrite(plan)
    }
}

impl PatternToProjectionRule {
    fn rewrite(&self, plan: LogicalPlan) -> DFResult<Transformed<LogicalPlan>> {
        Ok(plan.transform(|plan| {
            let new_plan = match plan {
                LogicalPlan::Extension(Extension { node })
                    if node.as_any().downcast_ref::<PatternNode>().is_some() =>
                {
                    let node = node.as_any().downcast_ref::<PatternNode>().unwrap();
                    let plan = node.input().clone();

                    let plan = filter_by_values(plan.into(), node.patterns())?;
                    let plan = filter_same_variable(plan, node.patterns())?;
                    let plan = project_to_variables(plan, node.patterns())?;

                    Transformed::yes(plan.build()?)
                }
                _ => Transformed::no(plan),
            };
            Ok(new_plan)
        })?)
    }
}

/// Adds filter operations that constraints the solutions of patterns that use literals.
///
/// For example, for the pattern `?a foaf:knows ?b` this functions adds a filter that ensures that
/// the predicate is `foaf:knows`.
fn filter_by_values(
    plan: LogicalPlanBuilder,
    pattern: &[TermPattern],
) -> DFResult<LogicalPlanBuilder> {
    let filter = plan
        .schema()
        .columns()
        .iter()
        .zip(pattern.iter())
        .filter_map(|(c, p)| value_pattern_to_filter_expr(c, p))
        .reduce(|lhs, rhs| and(lhs, rhs));

    let Some(filter) = filter else {
        return Ok(plan);
    };

    plan.filter(filter)
}

/// Returns a filter expression based on `column` (for the name) only if the given pattern is a
/// value (e.g., named node).
fn value_pattern_to_filter_expr(column: &Column, pattern: &TermPattern) -> Option<Expr> {
    let scalar = match pattern {
        TermPattern::NamedNode(nn) => Some(encode_scalar_named_node(nn.as_ref())),
        TermPattern::BlankNode(_) => None,
        TermPattern::Literal(lit) => Some(encode_scalar_literal(lit.as_ref()).unwrap()),
        TermPattern::Variable(_) => None,
        TermPattern::Triple(_) => unimplemented!(),
    }?;

    Some(ENC_AS_NATIVE_BOOLEAN.call(vec![
        ENC_SAME_TERM.call(vec![Expr::from(column.clone()), lit(scalar)]),
    ]))
}

/// Adds filter operations that constraints the solutions of patterns that use the same variable
/// twice.
///
/// For example, for the pattern `?a ?a ?b` this functions adds a constraint that ensures that the
/// subject is equal to the predicate.
fn filter_same_variable(
    plan: LogicalPlanBuilder,
    pattern: &[TermPattern],
) -> DFResult<LogicalPlanBuilder> {
    let mut mappings = HashMap::new();

    let column_patterns = plan.schema().columns().into_iter().zip(pattern.iter());
    for (column, pattern) in column_patterns {
        match pattern_to_variable_name(pattern) {
            Some(variable) => {
                if !mappings.contains_key(&variable) {
                    mappings.insert(variable.clone(), Vec::new());
                }
                mappings.get_mut(&variable).unwrap().push(column.clone());
            }
            None => {}
        }
    }

    let mut result_plan = plan;
    for value in mappings.into_values() {
        let columns = value.into_iter().map(|v| col(v)).collect::<Vec<_>>();
        let constraint = columns
            .iter()
            .zip(columns.iter().skip(1))
            .map(|(a, b)| {
                ENC_EFFECTIVE_BOOLEAN_VALUE
                    .call(vec![ENC_SAME_TERM.call(vec![a.clone(), b.clone()])])
            })
            .reduce(|a, b| a.and(b));
        result_plan = match constraint {
            Some(constraint) => result_plan.filter(constraint)?,
            _ => result_plan,
        }
    }

    Ok(result_plan)
}

/// Projects the inner columns to the variables.
fn project_to_variables(
    plan: LogicalPlanBuilder,
    patterns: &[TermPattern],
) -> DFResult<LogicalPlanBuilder> {
    let possible_projections = plan
        .schema()
        .columns()
        .into_iter()
        .zip(patterns.iter())
        .filter_map(|(c, p)| pattern_to_variable_name(p).map(|vname| (c, vname)));

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
