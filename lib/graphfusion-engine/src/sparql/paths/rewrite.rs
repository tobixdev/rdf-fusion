use crate::sparql::paths::PathNode;
use crate::DFResult;
use arrow_rdf::encoded::scalars::{
    encode_scalar_blank_node, encode_scalar_literal, encode_scalar_named_node,
    encode_scalar_predicate,
};
use arrow_rdf::encoded::{ENC_AS_NATIVE_BOOLEAN, ENC_EQ, ENC_QUAD_SCHEMA};
use arrow_rdf::{COL_GRAPH, COL_OBJECT, COL_SUBJECT, TABLE_QUADS};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, JoinType};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{
    col, lit, table_scan, Expr, Extension, LogicalPlan, LogicalPlanBuilder,
};
use datafusion::optimizer::AnalyzerRule;
use spargebra::algebra::PropertyPathExpression;
use spargebra::term::{NamedNodePattern, TermPattern};

#[derive(Default, Debug)]
pub struct PathToJoinsRule {}

impl AnalyzerRule for PathToJoinsRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        Self::analyze_plan(plan)
    }

    fn name(&self) -> &str {
        "path_to_joins_rule"
    }
}

impl PathToJoinsRule {
    fn analyze_plan(plan: LogicalPlan) -> DFResult<LogicalPlan> {
        Ok(plan
            .transform(|plan| {
                let new_plan = match plan {
                    LogicalPlan::Extension(Extension { node })
                        if node.as_any().downcast_ref::<PathNode>().is_some() =>
                    {
                        let node = node.as_any().downcast_ref::<PathNode>().unwrap();
                        let query = build_path_query(node.graph().as_ref(), node.path())?;
                        let query = filter_and_project_term(query, "start", node.subject())?;
                        let query = filter_and_project_term(query, "end", node.object())?;
                        let query = project_graph(query, node.graph().as_ref())?;
                        Transformed::yes(query.build()?)
                    }
                    _ => Transformed::no(plan),
                };
                Ok(new_plan)
            })?
            .data)
    }
}

/// The resulting query always has a column "start" and "end" that indicates the respective start
/// and end of the current path. In addition to that, the result contains a graph column and may
/// contain additional fields that can bind values to variables.
fn build_path_query(
    graph: Option<&NamedNodePattern>,
    path: &PropertyPathExpression,
) -> DFResult<LogicalPlanBuilder> {
    match path {
        PropertyPathExpression::NamedNode(node) => {
            let query =
                scan_quads(graph)?.filter(ENC_AS_NATIVE_BOOLEAN.call(vec![ENC_EQ.call(vec![
                    col(COL_GRAPH),
                    lit(encode_scalar_predicate(node.as_ref())),
                ])]))?;
            query.project([
                col(COL_GRAPH),
                col(COL_SUBJECT).alias("start"),
                col(COL_OBJECT).alias("end"),
            ])
        }
        PropertyPathExpression::Reverse(_) => todo!("Track state for Sequence operator"),
        PropertyPathExpression::Sequence(lhs, rhs) => {
            let lhs = build_path_query(graph, lhs.as_ref())?.alias("lhs")?;
            let rhs = build_path_query(graph, rhs.as_ref())?.alias("rhs")?;

            lhs.join_on(
                rhs.build()?,
                JoinType::Inner,
                [
                    ENC_AS_NATIVE_BOOLEAN.call(vec![ENC_EQ.call(vec![
                        Expr::from(Column {
                            relation: Some("lhs".into()),
                            name: String::from("graph"),
                        }),
                        Expr::from(Column {
                            relation: Some("rhs".into()),
                            name: String::from("graph"),
                        }),
                    ])]),
                    ENC_AS_NATIVE_BOOLEAN.call(vec![ENC_EQ.call(vec![
                        Expr::from(Column {
                            relation: Some("lhs".into()),
                            name: String::from("end"),
                        }),
                        Expr::from(Column {
                            relation: Some("rhs".into()),
                            name: String::from("start"),
                        }),
                    ])]),
                ],
            )
        }
        PropertyPathExpression::Alternative(lhs, rhs) => {
            let lhs = build_path_query(graph, lhs.as_ref())?;
            let rhs = build_path_query(graph, rhs.as_ref())?;
            lhs.union(rhs.build()?)
        }
        PropertyPathExpression::ZeroOrMore(_) => {
            todo!("Recursive CTE - include path length and filter")
        }
        PropertyPathExpression::OneOrMore(_) => {
            todo!("Recursive CTE - include path length and filter")
        }
        PropertyPathExpression::ZeroOrOne(_) => {
            todo!("Recusive CTE - include path length and filter")
        }
        PropertyPathExpression::NegatedPropertySet(names) => {
            todo!("Use not in")
        }
    }
}

/// Scans the quads table and optionally filters it to the given named graph.
fn scan_quads(graph: Option<&NamedNodePattern>) -> DFResult<LogicalPlanBuilder> {
    let query = table_scan(Some(TABLE_QUADS), ENC_QUAD_SCHEMA.as_ref(), None)?;
    let query = match graph {
        Some(NamedNodePattern::NamedNode(nn)) => {
            query.filter(ENC_AS_NATIVE_BOOLEAN.call(vec![ENC_EQ.call(vec![
                col(COL_GRAPH),
                lit(encode_scalar_named_node(nn.as_ref())),
            ])]))?
        }
        _ => query,
    };
    Ok(query)
}

/// Depending on `term`, this function either
/// - ... projects the `column` of a path to the respective variable
/// - ... filter the `column` of a path by the given term
fn filter_and_project_term(
    inner: LogicalPlanBuilder,
    column: &str,
    term: &TermPattern,
) -> DFResult<LogicalPlanBuilder> {
    match term {
        TermPattern::NamedNode(nn) => {
            inner.filter(ENC_AS_NATIVE_BOOLEAN.call(vec![ENC_EQ.call(vec![
                col(Column::new_unqualified(column)),
                lit(encode_scalar_named_node(nn.as_ref())),
            ])]))
        }
        TermPattern::BlankNode(bnode) => {
            inner.filter(ENC_AS_NATIVE_BOOLEAN.call(vec![ENC_EQ.call(vec![
                col(Column::new_unqualified(column)),
                lit(encode_scalar_blank_node(bnode.as_ref())),
            ])]))
        }
        TermPattern::Literal(literal) => {
            inner.filter(ENC_AS_NATIVE_BOOLEAN.call(vec![ENC_EQ.call(vec![
                col(Column::new_unqualified(column)),
                lit(encode_scalar_literal(literal.as_ref())?),
            ])]))
        }
        TermPattern::Variable(var) => {
            let columns = inner
                .schema()
                .fields()
                .iter()
                .map(|f| {
                    let expr = col(Column::new_unqualified(f.name()));
                    if f.name() == column {
                        expr.alias(var.as_str())
                    } else {
                        expr
                    }
                })
                .collect::<Vec<_>>();
            inner.project(columns)
        }
        TermPattern::Triple(_) => unimplemented!("Triple"),
    }
}

/// Projects the "graph" column to a variable if the pattern is a variable.
///
/// Handling querying a fixed graph is done in [build_path_query].
fn project_graph(
    query: LogicalPlanBuilder,
    graph: Option<&NamedNodePattern>,
) -> DFResult<LogicalPlanBuilder> {
    match graph {
        Some(NamedNodePattern::Variable(var)) => {
            let columns = query
                .schema()
                .fields()
                .iter()
                .map(|f| {
                    let expr = col(Column::new_unqualified(f.name()));
                    if f.name() == "graph" {
                        expr.alias(var.as_str())
                    } else {
                        expr
                    }
                })
                .collect::<Vec<_>>();
            query.project(columns)
        }
        _ => Ok(query),
    }
}
