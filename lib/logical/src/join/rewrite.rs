use crate::check_same_schema;
use crate::join::{SparqlJoinNode, SparqlJoinType};
use crate::RdfFusionExprBuilderRoot;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, ExprSchema, JoinType};
use datafusion::logical_expr::{Expr, UserDefinedLogicalNode};
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::EncodingName;
use rdf_fusion_functions::registry::RdfFusionFunctionRegistryRef;
use std::collections::HashSet;

/// TODO
#[derive(Debug)]
pub struct SparqlJoinLoweringRule {
    /// Used for creating expressions with RdfFusion builtins.
    registry: RdfFusionFunctionRegistryRef,
}

impl OptimizerRule for SparqlJoinLoweringRule {
    fn name(&self) -> &str {
        "sparql-join-lowering"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform(|plan| {
            let new_plan = match &plan {
                LogicalPlan::Extension(Extension { node }) => {
                    if let Some(node) = node.as_any().downcast_ref::<SparqlJoinNode>() {
                        let new_plan = self.rewrite_sparql_join(node)?;
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

impl SparqlJoinLoweringRule {
    /// TODO
    pub fn new(registry: RdfFusionFunctionRegistryRef) -> Self {
        Self { registry }
    }

    /// TODO
    fn rewrite_sparql_join(&self, node: &SparqlJoinNode) -> DFResult<LogicalPlan> {
        let (lhs_keys, rhs_keys) = get_join_keys(node);

        // If both solutions are disjoint and there is no filter, we can use a cross-join.
        if lhs_keys.is_disjoint(&rhs_keys) && node.filter().is_none() {
            return LogicalPlanBuilder::new(node.lhs().clone())
                .cross_join(node.rhs().clone())?
                .build();
        }

        let join_on = lhs_keys
            .intersection(&rhs_keys)
            .map(Column::new_unqualified)
            .collect::<Vec<_>>();

        // Try to reduce the SPARQL join to a regular join.
        if let Some(result) = self.try_build_regular_join(node, &join_on)? {
            return Ok(result);
        }

        // Otherwise, use a join with a filter that checks if the values are compatible.
        self.build_join_with_is_compatible(node, &join_on)
    }

    /// TODO
    fn try_build_regular_join(
        &self,
        node: &SparqlJoinNode,
        join_on: &[Column],
    ) -> DFResult<Option<LogicalPlan>> {
        let any_column_not_null = join_on
            .iter()
            .map(|col| {
                let lhs_field = node.lhs().schema().field_from_column(col)?;
                let rhs_field = node.rhs().schema().field_from_column(col)?;
                DFResult::Ok(lhs_field.is_nullable() || rhs_field.is_nullable())
            })
            .reduce(|l, r| Ok(l? || r?))
            .transpose()?;

        Ok(match any_column_not_null {
            // If no column is nullable, we can use a regular join.
            Some(false) => {
                let lhs = LogicalPlanBuilder::new(node.lhs().clone()).alias("lhs")?;
                let rhs = LogicalPlanBuilder::new(node.rhs().clone()).alias("rhs")?;
                let projections = self.create_join_projections(node, &lhs, &rhs, false)?;

                let filter = node
                    .filter()
                    .map(|f| self.rewrite_filter_for_join(node, f, false))
                    .transpose()?;
                let plan = lhs
                    .join(
                        rhs.build()?,
                        get_data_fusion_join_type(node),
                        (join_on.to_vec(), join_on.to_vec()),
                        filter,
                    )?
                    .project(projections)?
                    .build()?;
                Some(plan)
            }
            // If at least one column is nullable, we cannot use a regular join. Furthermore, if
            // there are no equi-join conditions, we skip this step.
            Some(true) | None => None,
        })
    }

    /// TODO
    fn build_join_with_is_compatible(
        &self,
        node: &SparqlJoinNode,
        join_on: &[Column],
    ) -> DFResult<LogicalPlan> {
        let lhs = LogicalPlanBuilder::new(node.lhs().clone()).alias("lhs")?;
        let rhs = LogicalPlanBuilder::new(node.rhs().clone()).alias("rhs")?;
        let projections = self.create_join_projections(node, &lhs, &rhs, true)?;

        let mut join_schema = lhs.schema().as_ref().clone();
        join_schema.merge(rhs.schema());
        let expr_builder_root = RdfFusionExprBuilderRoot::new(self.registry.as_ref(), &join_schema);

        let mut join_filters = join_on
            .iter()
            .map(|col| {
                expr_builder_root
                    .try_create_builder(Expr::from(col.with_relation("lhs".into())))?
                    .build_is_compatible(Expr::from(col.with_relation("rhs".into())))
            })
            .collect::<DFResult<Vec<_>>>()?;

        if let Some(filter) = node.filter() {
            let filter = self.rewrite_filter_for_join(node, filter, true)?;
            join_filters.push(filter);
        }
        let filter_expr = join_filters.into_iter().reduce(Expr::and);

        let join = lhs.join_detailed(
            rhs.build()?,
            get_data_fusion_join_type(node),
            (Vec::<Column>::new(), Vec::<Column>::new()),
            filter_expr,
            false,
        )?;

        join.project(projections)?.build()
    }

    /// TODO
    fn create_join_projections(
        &self,
        node: &SparqlJoinNode,
        lhs: &LogicalPlanBuilder,
        rhs: &LogicalPlanBuilder,
        requires_coalesce: bool,
    ) -> DFResult<Vec<Expr>> {
        let mut join_schema = lhs.schema().as_ref().clone();
        join_schema.merge(rhs.schema());
        let expr_builder_root = RdfFusionExprBuilderRoot::new(self.registry.as_ref(), &join_schema);

        let (lhs_keys, rhs_keys) = get_join_keys(node);
        let projections = node
            .schema()
            .columns()
            .into_iter()
            .map(|c| {
                value_from_joined(
                    expr_builder_root,
                    &lhs_keys,
                    &rhs_keys,
                    c.name(),
                    requires_coalesce,
                )
            })
            .collect::<DFResult<Vec<_>>>()?;

        Ok(projections)
    }

    /// TODO
    fn rewrite_filter_for_join(
        &self,
        node: &SparqlJoinNode,
        filter: &Expr,
        requires_coalesce: bool,
    ) -> DFResult<Expr> {
        let lhs = LogicalPlanBuilder::new(node.lhs().clone()).alias("lhs")?;
        let rhs = LogicalPlanBuilder::new(node.rhs().clone()).alias("rhs")?;

        let mut join_schema = lhs.schema().as_ref().clone();
        join_schema.merge(rhs.schema());
        let expr_builder_root = RdfFusionExprBuilderRoot::new(self.registry.as_ref(), &join_schema);

        let (lhs_keys, rhs_keys) = get_join_keys(node);
        let filter = filter
            .clone()
            .transform(|e| {
                Ok(match e {
                    Expr::Column(c) => Transformed::yes(value_from_joined(
                        expr_builder_root,
                        &lhs_keys,
                        &rhs_keys,
                        c.name(),
                        requires_coalesce,
                    )?),
                    _ => Transformed::no(e),
                })
            })?
            .data;
        Ok(filter)
    }
}

/// Returns an expression that obtains value `variable` from either the lhs, the rhs, or both
/// depending on the schema.
fn value_from_joined(
    expr_builder_root: RdfFusionExprBuilderRoot<'_>,
    lhs_keys: &HashSet<String>,
    rhs_keys: &HashSet<String>,
    variable: &str,
    requires_coalesce: bool,
) -> DFResult<Expr> {
    let lhs_expr = Expr::from(Column::new(Some("lhs"), variable));
    let rhs_expr = Expr::from(Column::new(Some("rhs"), variable));

    let expr = match (lhs_keys.contains(variable), rhs_keys.contains(variable)) {
        (true, true) => {
            if requires_coalesce {
                expr_builder_root
                    .try_create_builder(lhs_expr)?
                    .coalesce(vec![rhs_expr])?
                    .with_encoding(EncodingName::PlainTerm)?
                    .build()?
            } else {
                lhs_expr
            }
        }
        (true, false) => lhs_expr,
        (false, true) => rhs_expr,
        (false, false) => unreachable!("At least one of lhs or rhs must contain variable"),
    };
    Ok(expr.alias(variable))
}

/// Returns the DataFusion [JoinType] type corresponding to the given [SparqlJoinType].
fn get_data_fusion_join_type(node: &SparqlJoinNode) -> JoinType {
    match node.join_type() {
        SparqlJoinType::Inner => JoinType::Inner,
        SparqlJoinType::Left => JoinType::Left,
    }
}

fn get_join_keys(node: &SparqlJoinNode) -> (HashSet<String>, HashSet<String>) {
    let lhs_keys: HashSet<_> = node
        .lhs()
        .schema()
        .columns()
        .into_iter()
        .map(|c| c.name().to_owned())
        .collect();
    let rhs_keys: HashSet<_> = node
        .rhs()
        .schema()
        .columns()
        .into_iter()
        .map(|c| c.name().to_owned())
        .collect();
    (lhs_keys, rhs_keys)
}
