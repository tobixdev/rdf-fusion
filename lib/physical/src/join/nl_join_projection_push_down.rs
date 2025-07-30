use datafusion::arrow::datatypes::{Fields, Schema, SchemaRef};
use datafusion::common::alias::AliasGenerator;
use datafusion::common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion::common::JoinSide;
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use rdf_fusion_common::DFResult;
use std::sync::Arc;

/// Tries to push down projections from join filters that only depend on one side of the join.
///
/// This can be a crucial optimization for nested loop joins. By pushing these projections
/// down, even functions that only depend on one side of the join must be done for all row
/// combinations.
#[derive(Debug)]
pub struct NestedLoopJoinProjectionPushDown {}

impl NestedLoopJoinProjectionPushDown {
    /// Creates a new [NestedLoopJoinProjectionPushDown].
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for NestedLoopJoinProjectionPushDown {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for NestedLoopJoinProjectionPushDown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let alias_generator = AliasGenerator::new();
        plan.transform_up(|plan| {
            match plan.as_any().downcast_ref::<NestedLoopJoinExec>() {
                None => Ok(Transformed::no(plan)),
                Some(hash_join) => try_pushdown_join_filter(
                    Arc::clone(&plan),
                    hash_join,
                    &alias_generator,
                ),
            }
        })
        .map(|t| t.data)
    }
    fn name(&self) -> &str {
        "nl-join-projection-push-down"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Tries to push down parts of the filter.
///
/// See [JoinFilterRewriter] for details.
fn try_pushdown_join_filter(
    original_plan: Arc<dyn ExecutionPlan>,
    join: &NestedLoopJoinExec,
    alias_generator: &AliasGenerator,
) -> DFResult<Transformed<Arc<dyn ExecutionPlan>>> {
    // Projections must be updated, currently not implemented.
    if join.projection().is_some() {
        return Ok(Transformed::no(original_plan));
    }

    let Some(filter) = join.filter() else {
        return Ok(Transformed::no(original_plan));
    };

    let lhs_rewrite = try_pushdown_projection(
        Arc::clone(&join.right().schema()),
        Arc::clone(join.left()),
        JoinSide::Left,
        filter.clone(),
        &alias_generator,
    )?;
    // let rhs_rewrite = try_pushdown_projection(
    //     Arc::clone(&join.left().schema()),
    //     Arc::clone(join.right()),
    //     JoinSide::Right,
    //     lhs_rewrite.data.1,
    //     &alias_generator,
    // )?;
    if !lhs_rewrite.transformed
    /*&& !rhs_rewrite.transformed */
    {
        return Ok(Transformed::no(original_plan));
    }

    Ok(Transformed::yes(Arc::new(NestedLoopJoinExec::try_new(
        lhs_rewrite.data.0,
        original_plan,
        Some(lhs_rewrite.data.1),
        join.join_type(),
        None,
    )?)))
}

/// Tries to push down parts of `expr` into the `join_side`.
fn try_pushdown_projection(
    other_schema: SchemaRef,
    plan: Arc<dyn ExecutionPlan>,
    join_side: JoinSide,
    join_filter: JoinFilter,
    alias_generator: &AliasGenerator,
) -> DFResult<Transformed<(Arc<dyn ExecutionPlan>, JoinFilter)>> {
    let expr = join_filter.expression().clone();
    let original_plan_schema = plan.schema();
    let mut rewriter = JoinFilterRewriter::new(
        join_side,
        original_plan_schema.as_ref(),
        join_filter.column_indices().to_vec(),
        alias_generator,
    );
    let new_expr = expr.rewrite(&mut rewriter)?;

    if new_expr.transformed {
        let new_join_side = ProjectionExec::try_new(rewriter.join_side_projections, plan)?;
        let new_schema = Arc::clone(&new_join_side.schema());

        let (lhs_schema, rhs_schema) = match join_side {
            JoinSide::Left => (new_schema, other_schema),
            JoinSide::Right => (other_schema, new_schema),
            JoinSide::None => unreachable!("Mark join not supported"),
        };
        let intermediate_schema = rewriter
            .intermediate_column_indices
            .iter()
            .map(|ci| match ci.side {
                JoinSide::Left => lhs_schema.fields[ci.index].clone(),
                JoinSide::Right => rhs_schema.fields[ci.index].clone(),
                JoinSide::None => unreachable!("Mark join not supported"),
            })
            .collect::<Fields>();

        let join_filter = JoinFilter::new(
            new_expr.data,
            rewriter.intermediate_column_indices,
            Arc::new(Schema::new(intermediate_schema)),
        );
        Ok(Transformed::yes((Arc::new(new_join_side), join_filter)))
    } else {
        Ok(Transformed::no((plan, join_filter)))
    }
}

/// Implements the push-down machinery.
///
/// The rewriter starts at the top of the filter expression and traverses the expression tree. For
/// each (sub-)expression, the rewriter checks whether it only refers to one side of the join. If
/// this is never the case, no subexpressions of the filter can be pushed down. If there is a
/// subexpression that can be computed using only one side of the join, the entire subexpression is
/// pushed down to the join side.
struct JoinFilterRewriter<'a> {
    join_side: JoinSide,
    join_side_schema: &'a Schema,
    join_side_projections: Vec<(Arc<dyn PhysicalExpr>, String)>,
    intermediate_column_indices: Vec<ColumnIndex>,
    alias_generator: &'a AliasGenerator,
}

impl<'a> JoinFilterRewriter<'a> {
    /// Creates a new [JoinFilterRewriter].
    fn new(
        join_side: JoinSide,
        join_side_schema: &'a Schema,
        column_indices: Vec<ColumnIndex>,
        alias_generator: &'a AliasGenerator,
    ) -> Self {
        let projections = join_side_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                (
                    Arc::new(Column::new(field.name(), idx)) as Arc<dyn PhysicalExpr>,
                    field.name().to_string(),
                )
            })
            .collect();

        Self {
            join_side,
            join_side_schema,
            join_side_projections: projections,
            intermediate_column_indices: column_indices,
            alias_generator,
        }
    }

    /// Creates a new column in the current join side.
    fn create_new_column(
        &mut self,
        name: String,
        expr: Arc<dyn PhysicalExpr>,
    ) -> DFResult<usize> {
        // First, add a new projection. The expression must be rewritten, as it is no longer
        // executed against the filter schema.
        let new_idx = self.join_side_projections.len();
        let rewritten_expr = expr.transform_up(|expr| {
            Ok(match expr.as_any().downcast_ref::<Column>() {
                None => Transformed::no(expr),
                Some(column) => {
                    let intermediate_column = &self.intermediate_column_indices[column.index()];
                    assert_eq!(intermediate_column.side, self.join_side);

                    let join_side_index = intermediate_column.index;
                    let field = self.join_side_schema.field(join_side_index);
                    let new_column = Column::new(field.name(), join_side_index);
                    Transformed::yes(Arc::new(new_column) as Arc<dyn PhysicalExpr>)
                }
            })
        })?;
        self.join_side_projections.push((rewritten_expr.data, name));

        // Then, update the column indices
        let new_intermediate_idx = self.intermediate_column_indices.len();
        let idx = ColumnIndex {
            index: new_idx,
            side: self.join_side,
        };
        self.intermediate_column_indices.push(idx);

        Ok(new_intermediate_idx)
    }

    /// Checks whether the entire expression depends on the current join side.
    fn depends_on_other_join_side(
        &mut self,
        expr: &Arc<dyn PhysicalExpr>,
    ) -> DFResult<bool> {
        let mut result = false;
        expr.apply(|expr| match expr.as_any().downcast_ref::<Column>() {
            None => Ok(TreeNodeRecursion::Continue),
            Some(c) => {
                let column_index = &self.intermediate_column_indices[c.index()];
                if column_index.side != self.join_side {
                    result = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            }
        })?;

        Ok(result)
    }
}

impl TreeNodeRewriter for JoinFilterRewriter<'_> {
    type Node = Arc<dyn PhysicalExpr>;

    fn f_down(&mut self, node: Self::Node) -> DFResult<Transformed<Self::Node>> {
        if self.depends_on_other_join_side(&node)? {
            return Ok(Transformed::no(node));
        }

        let alias = self.alias_generator.next("join_proj_push_down");
        let idx = self.create_new_column(alias.clone(), node)?;

        Ok(Transformed::yes(
            Arc::new(Column::new(&alias, idx)) as Self::Node
        ))
    }
}
