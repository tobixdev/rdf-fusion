use crate::expr_builder::GraphFusionExprBuilder;
use crate::patterns::QuadPatternNode;
use crate::DFResult;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{and, col, Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::Expr;
use graphfusion_functions::registry::{GraphFusionFunctionRegistry, GraphFusionFunctionRegistryRef};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
                    if let Some(node) = node.as_any().downcast_ref::<QuadPatternNode>() {
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

/// Computes the filters that will be applied for a given [QuadPatternNode]. Callers can use this
/// function to only apply the filters of a pattern and ignore any projections to variables.
pub fn compute_filters_for_pattern(
    registry: &GraphFusionFunctionRegistry,
    node: &QuadPatternNode,
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
    pattern: &[PatternNodeElement],
) -> DFResult<Option<Expr>> {
    let filters = expr_builder
        .schema()
        .columns()
        .iter()
        .zip(pattern.iter())
        .map(|(c, p)| p.filter_expression(&expr_builder, c))
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
    pattern: &[PatternNodeElement],
) -> DFResult<Option<Expr>> {
    let mut mappings = HashMap::new();

    let column_patterns = expr_builder
        .schema()
        .columns()
        .into_iter()
        .zip(pattern.iter());
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

fn filter() {
    let graph_pattern = state
        .graph
        .as_ref()
        .filter(|_| !state.graph_is_out_of_scope)
        .map(|nn| PatternNodeElement::from(nn.clone()));

    match graph_pattern {
        None => {
            let plan = plan.project([col(COL_SUBJECT), col(COL_PREDICATE), col(COL_OBJECT)])?;
            let patterns = vec![
                PatternNodeElement::from(pattern.subject.clone()),
                PatternNodeElement::from(pattern.predicate.clone()),
                PatternNodeElement::from(pattern.object.clone()),
            ];
            Ok(LogicalPlanBuilder::new(LogicalPlan::Extension(Extension {
                node: Arc::new(QuadPatternNode::try_new(plan.build()?, patterns)?),
            })))
        }
        Some(graph_pattern) => {
            let patterns = vec![
                graph_pattern,
                PatternNodeElement::from(pattern.subject.clone()),
                PatternNodeElement::from(pattern.predicate.clone()),
                PatternNodeElement::from(pattern.object.clone()),
            ];
            Ok(LogicalPlanBuilder::new(LogicalPlan::Extension(Extension {
                node: Arc::new(QuadPatternNode::try_new(plan.build()?, patterns)?),
            })))
        }
    }
}


/// Adds filter operations that constraints the solutions of patterns to named graphs if necessary.
fn filter_by_named_graph(
    expr_builder: GraphFusionExprBuilder<'_>,
    plan: LogicalPlanBuilder,
    dataset: &QueryDataset,
    graph_pattern: Option<&NamedNodePattern>,
) -> DFResult<LogicalPlanBuilder> {
    match graph_pattern {
        None => plan.filter(create_filter_for_default_graph(
            dataset.default_graph_graphs(),
        )),
        Some(NamedNodePattern::Variable(_)) => plan.filter(create_filter_for_named_graph(
            dataset.available_named_graphs(),
        )),
        Some(NamedNodePattern::NamedNode(nn)) => {
            let graph_filter = expr_builder.filter_by_scalar(col(COL_GRAPH), nn.as_ref().into())?;
            plan.filter(graph_filter)
        }
    }
}


/// Creates an [Expr] that filters `column` based on the contents of this element.
#[allow(clippy::unwrap_in_result, reason = "TODO")]
pub fn filter_expression(
    &self,
    factory: &GraphFusionExprBuilder,
    column: &Column,
) -> DFResult<Option<Expr>> {
    let result = match self {
        PatternNodeElement::NamedNode(nn) => Some(
            factory.filter_by_scalar(col(column.clone()), Term::from(nn.clone()).as_ref())?,
        ),
        PatternNodeElement::Literal(lit) => Some(
            factory.filter_by_scalar(col(column.clone()), Term::from(lit.clone()).as_ref())?,
        ),
        PatternNodeElement::BlankNode(_) => {
            // A blank node indicates that this should be a non-default graph.
            Some(Expr::from(column.clone()).is_not_null())
        }
        PatternNodeElement::DefaultGraph => Some(Expr::from(column.clone()).is_null()),
        _ => None,
    };
    Ok(result)
}

/// Returns a reference to a possible variable.
pub fn variable_name(&self) -> Option<String> {
    match self {
        PatternNodeElement::BlankNode(bnode) => Some(format!("_:{}", bnode.as_ref().as_str())),
        PatternNodeElement::Variable(var) => Some(var.as_str().into()),
        _ => None,
    }
}

fn create_filter_for_default_graph(
    expr_builder: GraphFusionExprBuilder<'_>,
    graph: Option<&[GraphName]>,
) -> DFResult<Expr> {
    let Some(graph) = graph else {
        return Ok(not(
            expr_builder.effective_boolean_value(expr_builder.bound(col(COL_GRAPH))?)?
        ));
    };

    let filters = graph
        .iter()
        .map(|name| match name {
            GraphName::NamedNode(nn) => {
                expr_builder.filter_by_scalar(col(COL_GRAPH), nn.as_ref().into())
            }
            GraphName::BlankNode(bnode) => {
                expr_builder.filter_by_scalar(col(COL_GRAPH), bnode.as_ref().into())
            }
            GraphName::DefaultGraph => Ok(not(
                expr_builder.effective_boolean_value(expr_builder.bound(col(COL_GRAPH))?)?
            )),
        })
        .collect::<DFResult<Vec<_>>>()?;

    filters.into_iter().reduce(Expr::or).unwrap_or(lit(false))
}

fn create_filter_for_named_graph(
    expr_builder: GraphFusionExprBuilder<'_>,
    graphs: Option<&[NamedOrBlankNode]>,
) -> Expr {
    let Some(graphs) = graphs else {
        return ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![ENC_BOUND.call(vec![col(COL_GRAPH)])]);
    };

    graphs
        .iter()
        .map(|name| match name {
            NamedOrBlankNode::NamedNode(nn) => {
                expr_builder.filter_by_scalar(
                    col(COL_GRAPH),
                    nn.as_ref().into(),
                )?
            }
            NamedOrBlankNode::BlankNode(bnode) => {
                expr_builder.filter_by_scalar(
                    col(COL_GRAPH),
                    bnode.as_ref().into(),
                )
            }
        })
        .reduce(Expr::or)
        .unwrap_or(lit(false))
}
