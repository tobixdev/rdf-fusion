use crate::expr_builder::RdfFusionExprBuilder;
use crate::patterns::PatternNode;
use crate::{check_same_schema, RdfFusionExprBuilderContext};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{
    and, col, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::Expr;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_functions::registry::{RdfFusionFunctionRegistry, RdfFusionFunctionRegistryRef};
use rdf_fusion_model::{Term, TermPattern};
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct PatternLoweringRule {
    storage_encoding: QuadStorageEncoding,
    registry: RdfFusionFunctionRegistryRef,
}

impl PatternLoweringRule {
    /// Creates a new [PatternLoweringRule].
    pub fn new(
        storage_encoding: QuadStorageEncoding,
        registry: RdfFusionFunctionRegistryRef,
    ) -> Self {
        Self {
            storage_encoding,
            registry,
        }
    }
}

impl OptimizerRule for PatternLoweringRule {
    fn name(&self) -> &str {
        "pattern-node-lowering"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| {
            let new_plan = match &plan {
                LogicalPlan::Extension(Extension { node }) => {
                    if let Some(node) = node.as_any().downcast_ref::<PatternNode>() {
                        let plan = LogicalPlanBuilder::from(node.input().clone());

                        let filter = compute_filters_for_pattern(
                            self.registry.as_ref(),
                            self.storage_encoding.object_id_encoding(),
                            node,
                        )?;
                        let plan = match filter {
                            None => plan,
                            Some(filter) => plan.filter(filter)?,
                        };
                        let new_plan = project_to_variables(plan, node.patterns())?.build()?;

                        check_same_schema(node.schema(), new_plan.schema())?;
                        Transformed::yes(new_plan)
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
    registry: &dyn RdfFusionFunctionRegistry,
    object_id_encoding: Option<&ObjectIdEncoding>,
    node: &PatternNode,
) -> DFResult<Option<Expr>> {
    let expr_builder_root =
        RdfFusionExprBuilderContext::new(registry, object_id_encoding, node.input().schema());
    let filters = [
        filter_by_values(expr_builder_root, node.patterns())?,
        filter_same_variable(expr_builder_root, node.patterns())?,
    ];
    Ok(filters.into_iter().flatten().reduce(and))
}

/// Adds filter operations that constraints the solutions of patterns that use literals.
///
/// For example, for the pattern `?a foaf:knows ?b` this functions adds a filter that ensures that
/// the predicate is `foaf:knows`.
fn filter_by_values(
    expr_builder_root: RdfFusionExprBuilderContext<'_>,
    pattern: &[Option<TermPattern>],
) -> DFResult<Option<Expr>> {
    let filters = expr_builder_root
        .schema()
        .columns()
        .iter()
        .zip(pattern.iter())
        .map(|(c, p)| {
            let builder = expr_builder_root.try_create_builder(col(c.clone()))?;
            create_filter_expression(builder, p.as_ref())
        })
        .collect::<DFResult<Vec<_>>>()?;
    Ok(filters.into_iter().flatten().reduce(and))
}

/// Adds filter operations that constraints the solutions of patterns that use the same variable
/// twice.
///
/// For example, for the pattern `?a ?a ?b` this functions adds a constraint that ensures that the
/// subject is equal to the predicate.
fn filter_same_variable(
    expr_builder_root: RdfFusionExprBuilderContext<'_>,
    pattern: &[Option<TermPattern>],
) -> DFResult<Option<Expr>> {
    let mut mappings = HashMap::new();

    let column_patterns = expr_builder_root
        .schema()
        .columns()
        .into_iter()
        .zip(pattern.iter());
    for (column, pattern) in column_patterns {
        // TODO: Support blank nodes?
        if let Some(TermPattern::Variable(variable)) = pattern {
            if !mappings.contains_key(variable) {
                mappings.insert(variable.clone(), Vec::new());
            }
            mappings.get_mut(variable).unwrap().push(column.clone());
        }
    }

    let mut constraints = Vec::new();
    for value in mappings.into_values() {
        let columns = value.into_iter().map(col).collect::<Vec<_>>();
        let new_constraints = columns
            .iter()
            .zip(columns.iter().skip(1))
            .map(|(a, b)| {
                expr_builder_root
                    .try_create_builder(a.clone())?
                    .build_same_term(b.clone())
            })
            .collect::<DFResult<Vec<_>>>()?;

        let new_constraint = new_constraints.into_iter().reduce(Expr::and);
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
            Some(TermPattern::Variable(v)) => Some((c, v.as_str())),
            Some(TermPattern::BlankNode(bnode)) => Some((c, bnode.as_str())),
            _ => None,
        });

    let mut already_projected = HashSet::new();
    let mut projections = Vec::new();
    for (old_name, new_name) in possible_projections {
        if !already_projected.contains(new_name) {
            already_projected.insert(new_name);

            let expr = Expr::from(old_name).alias(new_name);
            projections.push(expr);
        }
    }

    plan.project(projections)
}

/// Creates an [Expr] that filters `column` based on the contents of this element.
fn create_filter_expression(
    expr_builder: RdfFusionExprBuilder<'_>,
    pattern: Option<&TermPattern>,
) -> DFResult<Option<Expr>> {
    match pattern {
        Some(TermPattern::NamedNode(nn)) => {
            Some(expr_builder.build_same_term_scalar(Term::from(nn.clone()).as_ref())).transpose()
        }
        Some(TermPattern::Literal(lit)) => {
            Some(expr_builder.build_same_term_scalar(Term::from(lit.clone()).as_ref())).transpose()
        }
        _ => Ok(None),
    }
}
