use datafusion::common::DFSchema;
use datafusion::common::alias::AliasGenerator;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion::logical_expr::Join;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, col};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use rdf_fusion_common::DFResult;
use std::sync::Arc;

/// Tries to push down projections from join filters that only depend on one side of the join.
#[derive(Debug)]
pub struct JoinProjectionPushDownRule {}

impl JoinProjectionPushDownRule {
    /// Creates a new [JoinProjectionPushDownRule].
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for JoinProjectionPushDownRule {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizerRule for JoinProjectionPushDownRule {
    fn name(&self) -> &str {
        "join-projection-push-down"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform_up(|plan| match plan {
            LogicalPlan::Join(join) => {
                let result = try_pushdown_join_filter(join, config.alias_generator())?;
                result.map_data(|j| Ok(LogicalPlan::Join(j)))
            }
            _ => Ok(Transformed::no(plan)),
        })
    }
}

fn try_pushdown_join_filter(
    join: Join,
    alias_generator: &AliasGenerator,
) -> DFResult<Transformed<Join>> {
    let Some(filter) = &join.filter else {
        return Ok(Transformed::no(join));
    };

    let lhs_rewrite =
        try_pushdown_projection(&join.left, filter.clone(), alias_generator)?;
    let rhs_rewrite =
        try_pushdown_projection(&join.right, lhs_rewrite.data.1, alias_generator)?;

    let new_filter = rhs_rewrite.data.1;
    Ok(match (lhs_rewrite.transformed, rhs_rewrite.transformed) {
        (false, false) => Transformed::no(join),
        (true, false) => Transformed::yes(Join::try_new(
            lhs_rewrite.data.0,
            join.right,
            join.on,
            Some(new_filter),
            join.join_type,
            join.join_constraint,
            join.null_equality,
        )?),
        (false, true) => Transformed::yes(Join::try_new(
            join.left,
            rhs_rewrite.data.0,
            join.on,
            Some(new_filter),
            join.join_type,
            join.join_constraint,
            join.null_equality,
        )?),
        (true, true) => Transformed::yes(Join::try_new(
            lhs_rewrite.data.0,
            rhs_rewrite.data.0,
            join.on,
            Some(new_filter),
            join.join_type,
            join.join_constraint,
            join.null_equality,
        )?),
    })
}

/// TODO
fn try_pushdown_projection(
    join_side: &Arc<LogicalPlan>,
    expr: Expr,
    alias_generator: &AliasGenerator,
) -> DFResult<Transformed<(Arc<LogicalPlan>, Expr)>> {
    let join_side = Arc::clone(join_side);
    let mut rewriter = JoinFilterRewriter::new(join_side.schema(), alias_generator);
    let new_expr = expr.rewrite(&mut rewriter)?;

    if new_expr.transformed {
        let projections = rewriter
            .projections
            .expect("Transformed expr must have projections.");
        let new_join_side = LogicalPlanBuilder::new_from_arc(join_side)
            .project(projections)?
            .build()?;
        Ok(Transformed::yes((Arc::new(new_join_side), new_expr.data)))
    } else {
        Ok(Transformed::no((join_side, new_expr.data)))
    }
}

struct JoinFilterRewriter<'a> {
    schema: &'a DFSchema,
    alias_generator: &'a AliasGenerator,
    projections: Option<Vec<Expr>>,
}

impl<'a> JoinFilterRewriter<'a> {
    /// Creates a new [JoinFilterRewriter].
    fn new(schema: &'a DFSchema, alias_generator: &'a AliasGenerator) -> Self {
        Self {
            schema,
            alias_generator,
            projections: None,
        }
    }
}

impl TreeNodeRewriter for JoinFilterRewriter<'_> {
    type Node = Expr;

    fn f_down(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        // Rewriting "concludes" once a push-down has been applied.
        if self.projections.is_some() {
            return Ok(Transformed::no(node));
        }

        let depends_on_other_side = node
            .column_refs()
            .iter()
            .any(|c| !self.schema.has_column(c));
        if depends_on_other_side {
            return Ok(Transformed::no(node));
        }

        let alias = self.alias_generator.next("join_proj_push_down");
        let push_down_expr = node.clone().alias(&alias);

        let mut projections = Vec::new();
        projections.extend(self.schema.columns().iter().map(|c| col(c.clone())));
        projections.push(push_down_expr);
        self.projections = Some(projections);

        Ok(Transformed::yes(col(alias)))
    }
}
