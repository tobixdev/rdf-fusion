use datafusion::common::tree_node::Transformed;
use datafusion::common::Column;
use datafusion::logical_expr::Expr;
use std::collections::HashSet;
use crate::GraphFusionExprBuilder;

fn test() {
    let lhs = lhs.alias("lhs")?;
    let rhs = rhs.alias("rhs")?;
    let lhs_keys: HashSet<_> = lhs
        .schema()
        .columns()
        .into_iter()
        .map(|c| c.name().to_owned())
        .collect();
    let rhs_keys: HashSet<_> = rhs
        .schema()
        .columns()
        .into_iter()
        .map(|c| c.name().to_owned())
        .collect();

    // If both solutions are disjoint, we can use a cross join.
    if lhs_keys.is_disjoint(&rhs_keys) && filter.is_none() {
        return lhs.cross_join(rhs.build()?);
    }

    let mut join_schema = lhs.schema().as_ref().clone();
    join_schema.merge(rhs.schema());

    let mut join_filters = lhs_keys
        .intersection(&rhs_keys)
        .map(|k| {
            expr_builder.is_compatible(
                Expr::from(Column::new(Some("lhs"), k)),
                Expr::from(Column::new(Some("rhs"), k)),
            )
        })
        .collect::<DFResult<Vec<_>>>()?;
    if let Some(filter) = filter {
        let filter = filter
            .transform(|e| {
                Ok(match e {
                    Expr::Column(c) => {
                        Transformed::yes(value_from_joined(&lhs_keys, &rhs_keys, c.name()))
                    }
                    _ => Transformed::no(e),
                })
            })?
            .data;
        let filter = expr_builder.effective_boolean_value(filter)?;
        join_filters.push(filter);
    }
    let filter_expr = join_filters.into_iter().reduce(Expr::and);

    let projections = lhs_keys
        .union(&rhs_keys)
        .map(|k| value_from_joined(&lhs_keys, &rhs_keys, k))
        .collect::<Vec<_>>();

    lhs.join_detailed(
        rhs.build()?,
        join_type,
        (Vec::<Column>::new(), Vec::<Column>::new()),
        filter_expr,
        false,
    )?
    .project(projections)
}



/// Returns an expression that obtains value `variable` from either the lhs, the rhs, or both
/// depending on the schema.
fn value_from_joined(
    expr_builder: GraphFusionExprBuilder<'_>,
    lhs_keys: &HashSet<String>,
    rhs_keys: &HashSet<String>,
    variable: &str,
) -> Expr {
    let lhs_expr = Expr::from(Column::new(Some("lhs"), variable));
    let rhs_expr = Expr::from(Column::new(Some("rhs"), variable));

    match (lhs_keys.contains(variable), rhs_keys.contains(variable)) {
        (true, true) => expr_builder.coalesce(vec![lhs_expr, rhs_expr]),
        (true, false) => lhs_expr,
        (false, true) => rhs_expr,
        (false, false) => expr_builder.null_literal(),
    }
        .alias(variable)
}
