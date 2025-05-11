use datafusion::logical_expr::{col, lit, not, Expr, LogicalPlanBuilder};
use spargebra::algebra::QueryDataset;
use spargebra::term::NamedNodePattern;
use graphfusion_encoding::COL_GRAPH;
use graphfusion_model::{GraphName, NamedOrBlankNode};
use crate::{DFResult, GraphFusionExprBuilder};

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
