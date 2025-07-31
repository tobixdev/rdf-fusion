use datafusion::arrow::datatypes::{Fields, Schema, SchemaRef};
use datafusion::common::alias::AliasGenerator;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{JoinSide, JoinType};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::Volatility;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion::physical_plan::joins::NestedLoopJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use rdf_fusion_common::DFResult;
use std::collections::HashSet;
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
    let projections = join.projection();
    let Some(filter) = join.filter() else {
        return Ok(Transformed::no(original_plan));
    };

    // Mark joins are currently not supported.
    if matches!(join.join_type(), JoinType::LeftMark | JoinType::RightMark) {
        return Ok(Transformed::no(original_plan));
    }

    let original_lhs_length = join.left().schema().fields().len();
    let original_rhs_length = join.right().schema().fields().len();

    let lhs_rewrite = try_pushdown_projection(
        Arc::clone(&join.right().schema()),
        Arc::clone(join.left()),
        JoinSide::Left,
        filter.clone(),
        alias_generator,
    )?;
    let rhs_rewrite = try_pushdown_projection(
        Arc::clone(&lhs_rewrite.data.0.schema()),
        Arc::clone(join.right()),
        JoinSide::Right,
        lhs_rewrite.data.1,
        alias_generator,
    )?;
    if !lhs_rewrite.transformed && !rhs_rewrite.transformed {
        return Ok(Transformed::no(original_plan));
    }

    let join_filter = minimize_join_filter(
        Arc::clone(rhs_rewrite.data.1.expression()),
        rhs_rewrite.data.1.column_indices().to_vec(),
        lhs_rewrite.data.0.schema().as_ref(),
        rhs_rewrite.data.0.schema().as_ref(),
    );

    let new_lhs_length = lhs_rewrite.data.0.schema().fields.len();
    let projections = match projections {
        None => match join.join_type() {
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                // Build projections that ignore the newly projected columns.
                let mut projections = Vec::new();
                projections.extend(0..original_lhs_length);
                projections.extend(new_lhs_length..new_lhs_length + original_rhs_length);
                projections
            }
            JoinType::LeftSemi | JoinType::LeftAnti => {
                // Only return original left columns
                let mut projections = Vec::new();
                projections.extend(0..original_lhs_length);
                projections
            }
            JoinType::RightSemi | JoinType::RightAnti => {
                // Only return original right columns
                let mut projections = Vec::new();
                projections.extend(0..original_rhs_length);
                projections
            }
            _ => unreachable!("Unsupported join type"),
        },
        Some(projections) => {
            let rhs_offset = new_lhs_length - original_lhs_length;
            projections
                .iter()
                .map(|idx| {
                    if *idx >= original_lhs_length {
                        idx + rhs_offset
                    } else {
                        *idx
                    }
                })
                .collect()
        }
    };

    Ok(Transformed::yes(Arc::new(NestedLoopJoinExec::try_new(
        lhs_rewrite.data.0,
        rhs_rewrite.data.0,
        Some(join_filter),
        join.join_type(),
        Some(projections),
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
    let new_expr = rewriter.rewrite(expr)?;

    if new_expr.transformed {
        let new_join_side =
            ProjectionExec::try_new(rewriter.join_side_projections, plan)?;
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

/// Creates a new [JoinFilter] and tries to minimize the internal schema.
fn minimize_join_filter(
    expr: Arc<dyn PhysicalExpr>,
    old_column_indices: Vec<ColumnIndex>,
    lhs_schema: &Schema,
    rhs_schema: &Schema,
) -> JoinFilter {
    let mut used_columns = HashSet::new();
    expr.apply(|expr| {
        if let Some(col) = expr.as_any().downcast_ref::<Column>() {
            used_columns.insert(col.index());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("Closure cannot fail");

    let new_column_indices = old_column_indices
        .iter()
        .enumerate()
        .filter(|(idx, _)| used_columns.contains(idx))
        .map(|(_, ci)| ci.clone())
        .collect::<Vec<_>>();
    let fields = new_column_indices
        .iter()
        .map(|ci| match ci.side {
            JoinSide::Left => lhs_schema.field(ci.index).clone(),
            JoinSide::Right => rhs_schema.field(ci.index).clone(),
            JoinSide::None => unreachable!("Mark join not supported"),
        })
        .collect::<Fields>();

    let final_expr = expr
        .transform_up(|expr| match expr.as_any().downcast_ref::<Column>() {
            None => Ok(Transformed::no(expr)),
            Some(column) => {
                let new_idx = used_columns
                    .iter()
                    .filter(|idx| **idx < column.index())
                    .count();
                let new_column = Column::new(column.name(), new_idx);
                Ok(Transformed::yes(
                    Arc::new(new_column) as Arc<dyn PhysicalExpr>
                ))
            }
        })
        .expect("Closure cannot fail");

    JoinFilter::new(
        final_expr.data,
        new_column_indices,
        Arc::new(Schema::new(fields)),
    )
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

    /// Executes the push-down machinery on `expr`.
    ///
    /// See the [JoinFilterRewriter] for further information.
    fn rewrite(
        &mut self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> DFResult<Transformed<Arc<dyn PhysicalExpr>>> {
        let depends_on_this_side = self.depends_on_join_side(&expr, self.join_side)?;
        // We don't push down things that do not depend on this side (other side or no side).
        if !depends_on_this_side {
            return Ok(Transformed::no(expr));
        }

        // Recurse if there is a dependency to both sides or if the entire expression is volatile.
        let depends_on_other_side =
            self.depends_on_join_side(&expr, self.join_side.negate())?;
        let is_volatile = is_volatile(expr.as_ref());
        if depends_on_other_side || is_volatile {
            return expr.map_children(|expr| self.rewrite(expr));
        }

        // There is only a dependency on this side.

        // If this expression has no children, we do not push down, as it should already be a column
        // reference.
        if expr.children().is_empty() {
            return Ok(Transformed::no(expr));
        }

        // Otherwise, we push down a projection.
        let alias = self.alias_generator.next("join_proj_push_down");
        let idx = self.create_new_column(alias.clone(), expr)?;

        Ok(Transformed::yes(
            Arc::new(Column::new(&alias, idx)) as Arc<dyn PhysicalExpr>
        ))
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
                    let intermediate_column =
                        &self.intermediate_column_indices[column.index()];
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
    fn depends_on_join_side(
        &mut self,
        expr: &Arc<dyn PhysicalExpr>,
        join_side: JoinSide,
    ) -> DFResult<bool> {
        let mut result = false;
        expr.apply(|expr| match expr.as_any().downcast_ref::<Column>() {
            None => Ok(TreeNodeRecursion::Continue),
            Some(c) => {
                let column_index = &self.intermediate_column_indices[c.index()];
                if column_index.side == join_side {
                    result = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            }
        })?;

        Ok(result)
    }
}

fn is_volatile(expr: &dyn PhysicalExpr) -> bool {
    match expr.as_any().downcast_ref::<ScalarFunctionExpr>() {
        None => expr
            .children()
            .iter()
            .map(|expr| is_volatile(expr.as_ref()))
            .reduce(|lhs, rhs| lhs || rhs)
            .unwrap_or(false),
        Some(expr) => expr.fun().signature().volatility == Volatility::Volatile,
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
    use datafusion::common::{JoinSide, JoinType};
    use datafusion::functions::math::random;
    use datafusion::physical_expr::expressions::{binary, lit, Column};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::joins::utils::{ColumnIndex, JoinFilter};
    use datafusion::physical_plan::joins::NestedLoopJoinExec;
    use insta::assert_snapshot;
    use rdf_fusion_common::DFResult;
    use std::sync::Arc;

    #[tokio::test]
    async fn no_computation_does_not_project() -> DFResult<()> {
        let (left_schema, right_schema) = create_simple_schemas();
        let optimized_plan = run_test(
            left_schema,
            right_schema,
            join_first_column_indices(),
            a_greater_than_b,
            JoinType::Inner,
        )?;

        assert_snapshot!(optimized_plan, @r"
        NestedLoopJoinExec: join_type=Inner, filter=A@0 > B@1
          EmptyExec
          EmptyExec
        ");
        Ok(())
    }

    #[tokio::test]
    async fn simple_pushed_down_projections() -> DFResult<()> {
        let (left_schema, right_schema) = create_simple_schemas();
        let optimized_plan = run_test(
            left_schema,
            right_schema,
            join_first_column_indices(),
            a_plus_one_greater_than_b_plus_one,
            JoinType::Inner,
        )?;

        assert_snapshot!(optimized_plan, @r"
        NestedLoopJoinExec: join_type=Inner, filter=join_proj_push_down_1@0 > join_proj_push_down_2@1, projection=[A@0, B@2]
          ProjectionExec: expr=[A@0 as A, A@0 + 1 as join_proj_push_down_1]
            EmptyExec
          ProjectionExec: expr=[B@0 as B, B@0 + 1 as join_proj_push_down_2]
            EmptyExec
        ");
        Ok(())
    }

    #[tokio::test]
    async fn does_not_pushdown_volatile_functions() -> DFResult<()> {
        let (left_schema, right_schema) = create_simple_schemas();
        let optimized_plan = run_test(
            left_schema,
            right_schema,
            join_first_column_indices(),
            a_plus_rand_greater_than_b,
            JoinType::Inner,
        )?;

        assert_snapshot!(optimized_plan, @r"
        NestedLoopJoinExec: join_type=Inner, filter=A@0 + rand() > B@1
          EmptyExec
          EmptyExec
        ");
        Ok(())
    }

    #[tokio::test]
    async fn complex_schema_pushdown() -> DFResult<()> {
        // Left schema: (a, b, c)
        let left_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        // Right schema: (x, y, z)
        let right_schema = Schema::new(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
            Field::new("z", DataType::Int32, false),
        ]);

        // The filter will see the columns as [a, b, x, z]
        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
            ColumnIndex {
                index: 2,
                side: JoinSide::Right,
            },
        ];

        let optimized_plan = run_test(
            left_schema,
            right_schema,
            column_indices,
            |join_schema| {
                // Filter expression: a + b > x + z
                let a_col = Arc::new(Column::new("a", 0));
                let b_col = Arc::new(Column::new("b", 1));
                let x_col = Arc::new(Column::new("x", 2));
                let z_col = Arc::new(Column::new("z", 3));
                let lhs_expr = binary(
                    a_col,
                    datafusion::logical_expr::Operator::Plus,
                    b_col,
                    join_schema,
                )?;
                let rhs_expr = binary(
                    x_col,
                    datafusion::logical_expr::Operator::Plus,
                    z_col,
                    join_schema,
                )?;
                binary(
                    lhs_expr,
                    datafusion::logical_expr::Operator::Gt,
                    rhs_expr,
                    join_schema,
                )
            },
            JoinType::Inner,
        )?;

        assert_snapshot!(optimized_plan, @r"
        NestedLoopJoinExec: join_type=Inner, filter=join_proj_push_down_1@0 > join_proj_push_down_2@1, projection=[a@0, b@1, c@2, x@4, y@5, z@6]
          ProjectionExec: expr=[a@0 as a, b@1 as b, c@2 as c, a@0 + b@1 as join_proj_push_down_1]
            EmptyExec
          ProjectionExec: expr=[x@0 as x, y@1 as y, z@2 as z, x@0 + z@2 as join_proj_push_down_2]
            EmptyExec
        ");
        Ok(())
    }

    #[tokio::test]
    async fn left_semi_join_projection() -> DFResult<()> {
        let (left_schema, right_schema) = create_simple_schemas();

        let left_semi_join_plan = run_test(
            left_schema.clone(),
            right_schema.clone(),
            join_first_column_indices(),
            a_plus_one_greater_than_b_plus_one,
            JoinType::LeftSemi,
        )?;

        assert_snapshot!(left_semi_join_plan, @r"
        NestedLoopJoinExec: join_type=LeftSemi, filter=join_proj_push_down_1@0 > join_proj_push_down_2@1, projection=[A@0]
          ProjectionExec: expr=[A@0 as A, A@0 + 1 as join_proj_push_down_1]
            EmptyExec
          ProjectionExec: expr=[B@0 as B, B@0 + 1 as join_proj_push_down_2]
            EmptyExec
        ");
        Ok(())
    }

    #[tokio::test]
    async fn right_semi_join_projection() -> DFResult<()> {
        let (left_schema, right_schema) = create_simple_schemas();
        let right_semi_join_plan = run_test(
            left_schema,
            right_schema,
            join_first_column_indices(),
            a_plus_one_greater_than_b_plus_one,
            JoinType::RightSemi,
        )?;
        assert_snapshot!(right_semi_join_plan, @r"
        NestedLoopJoinExec: join_type=RightSemi, filter=join_proj_push_down_1@0 > join_proj_push_down_2@1, projection=[B@0]
          ProjectionExec: expr=[A@0 as A, A@0 + 1 as join_proj_push_down_1]
            EmptyExec
          ProjectionExec: expr=[B@0 as B, B@0 + 1 as join_proj_push_down_2]
            EmptyExec
        ");
        Ok(())
    }

    fn run_test(
        left_schema: Schema,
        right_schema: Schema,
        column_indices: Vec<ColumnIndex>,
        filter_expr_builder: impl FnOnce(&Schema) -> DFResult<Arc<dyn PhysicalExpr>>,
        join_type: JoinType,
    ) -> DFResult<String> {
        let left = Arc::new(EmptyExec::new(Arc::new(left_schema.clone())));
        let right = Arc::new(EmptyExec::new(Arc::new(right_schema.clone())));

        let join_fields: Vec<_> = column_indices
            .iter()
            .map(|ci| match ci.side {
                JoinSide::Left => left_schema.field(ci.index).clone(),
                JoinSide::Right => right_schema.field(ci.index).clone(),
                JoinSide::None => unreachable!(),
            })
            .collect();
        let join_schema = Arc::new(Schema::new(join_fields));

        let filter_expr = filter_expr_builder(join_schema.as_ref())?;

        let join_filter = JoinFilter::new(filter_expr, column_indices, join_schema);

        let join = NestedLoopJoinExec::try_new(
            left,
            right,
            Some(join_filter),
            &join_type,
            None,
        )?;

        let optimizer = NestedLoopJoinProjectionPushDown::new();
        let optimized_plan = optimizer.optimize(Arc::new(join), &Default::default())?;

        Ok(displayable(optimized_plan.as_ref())
            .indent(false)
            .to_string())
    }

    fn create_simple_schemas() -> (Schema, Schema) {
        let left_schema = Schema::new(vec![Field::new("A", DataType::Int32, false)]);
        let right_schema = Schema::new(vec![Field::new("B", DataType::Int32, false)]);

        (left_schema, right_schema)
    }

    fn join_first_column_indices() -> Vec<ColumnIndex> {
        vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ]
    }

    fn a_plus_one_greater_than_b_plus_one(
        join_schema: &Schema,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        let left_expr = binary(
            Arc::new(Column::new("A", 0)),
            datafusion::logical_expr::Operator::Plus,
            lit(1),
            &join_schema,
        )?;
        let right_expr = binary(
            Arc::new(Column::new("B", 1)),
            datafusion::logical_expr::Operator::Plus,
            lit(1),
            &join_schema,
        )?;
        binary(
            left_expr,
            datafusion::logical_expr::Operator::Gt,
            right_expr,
            &join_schema,
        )
    }

    fn a_plus_rand_greater_than_b(
        join_schema: &Schema,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        let left_expr = binary(
            Arc::new(Column::new("A", 0)),
            datafusion::logical_expr::Operator::Plus,
            Arc::new(ScalarFunctionExpr::new(
                "rand",
                random(),
                vec![],
                FieldRef::new(Field::new("out", DataType::Float64, false)),
            )),
            join_schema,
        )?;
        let right_expr = Arc::new(Column::new("B", 1));
        binary(
            left_expr,
            datafusion::logical_expr::Operator::Gt,
            right_expr,
            &join_schema,
        )
    }

    fn a_greater_than_b(join_schema: &Schema) -> DFResult<Arc<dyn PhysicalExpr>> {
        binary(
            Arc::new(Column::new("A", 0)),
            datafusion::logical_expr::Operator::Gt,
            Arc::new(Column::new("B", 1)),
            &join_schema,
        )
    }
}
