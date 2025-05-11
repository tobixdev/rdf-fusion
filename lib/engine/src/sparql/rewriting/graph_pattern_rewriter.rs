use crate::sparql::rewriting::expression_rewriter::ExpressionRewriter;
use crate::sparql::QueryDataset;
use crate::DFResult;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{not_impl_err, plan_datafusion_err, plan_err, Column, DFSchema};
use datafusion::datasource::TableProvider;
use datafusion::functions_aggregate::count::{count, count_udaf};
use datafusion::logical_expr::utils::COUNT_STAR_EXPANSION;
use datafusion::logical_expr::{Expr, LogicalPlan, SortExpr, UserDefinedLogicalNode};
use datafusion::prelude::col;
use graphfusion_encoding::EncodingName;
use graphfusion_functions::registry::GraphFusionFunctionRegistryRef;
use graphfusion_logical::join::SparqlJoinType;
use graphfusion_logical::{GraphFusionExprBuilder, GraphFusionLogicalPlanBuilder};
use graphfusion_model::Iri;
use graphfusion_model::Variable;
use spargebra::algebra::{
    AggregateExpression, AggregateFunction, Expression, GraphPattern, OrderExpression,
};
use spargebra::term::GraphNamePattern;
use std::cell::RefCell;
use std::sync::Arc;

pub struct GraphPatternRewriter {
    registry: GraphFusionFunctionRegistryRef,
    dataset: QueryDataset,
    base_iri: Option<Iri<String>>,
    // TODO: Check if we can remove this and just use TABLE_QUADS in the logical plan
    quads_table: Arc<dyn TableProvider>,
    state: RefCell<RewritingState>,
}

impl GraphPatternRewriter {
    pub fn new(
        registry: GraphFusionFunctionRegistryRef,
        dataset: QueryDataset,
        base_iri: Option<Iri<String>>,
        quads_table: Arc<dyn TableProvider>,
    ) -> Self {
        Self {
            registry,
            dataset,
            base_iri,
            quads_table,
            state: RefCell::default(),
        }
    }

    pub fn rewrite(&self, pattern: &GraphPattern) -> DFResult<LogicalPlan> {
        let plan = self.rewrite_graph_pattern(pattern)?;
        plan.build()
    }

    fn rewrite_graph_pattern(
        &self,
        pattern: &GraphPattern,
    ) -> DFResult<GraphFusionLogicalPlanBuilder> {
        match pattern {
            GraphPattern::Bgp { patterns } => {
                let state = self.state.borrow();
                GraphFusionLogicalPlanBuilder::new_from_bgp(
                    Arc::clone(&self.registry),
                    &state.graph,
                    patterns,
                )
            }
            GraphPattern::Project { inner, variables } => {
                if self.graph_variable_goes_out_of_scope(variables) {
                    let old_state = self.state.borrow().clone();
                    let new_state = old_state.with_graph_variable_going_out_of_scope();
                    self.state.replace(new_state);

                    let inner = self.rewrite_graph_pattern(inner.as_ref())?;
                    let result = inner.project(variables);

                    self.state.replace(old_state);
                    result
                } else {
                    let inner = self.rewrite_graph_pattern(inner.as_ref())?;
                    inner.project(variables)
                }
            }
            GraphPattern::Filter { inner, expr } => {
                let inner = self.rewrite_graph_pattern(inner.as_ref())?;
                let expr = self.rewrite_expression(inner.expr_builder(), expr)?;
                inner.filter(expr)
            }
            GraphPattern::Extend {
                inner,
                expression,
                variable,
            } => {
                let inner = self.rewrite_graph_pattern(inner)?;
                let expr = self.rewrite_expression(inner.expr_builder(), expression)?;
                inner.extend(variable.clone(), expr)
            }
            GraphPattern::Values {
                variables,
                bindings,
            } => GraphFusionLogicalPlanBuilder::new_from_values(
                Arc::clone(&self.registry),
                variables,
                bindings,
            ),
            GraphPattern::Join { left, right } => {
                let left = self.rewrite_graph_pattern(left)?;
                let right = self.rewrite_graph_pattern(right)?;
                left.join(right.build()?, SparqlJoinType::Inner, None)
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                let lhs = self.rewrite_graph_pattern(left)?;
                let rhs = self.rewrite_graph_pattern(right)?;

                let mut join_schema = lhs.schema().as_ref().clone();
                join_schema.merge(rhs.schema());

                let expr_builder = GraphFusionExprBuilder::new(&join_schema, &self.registry);
                let filter = expression
                    .as_ref()
                    .map(|f| self.rewrite_expression(expr_builder, f))
                    .transpose()?;

                lhs.join(rhs.build()?, SparqlJoinType::Left, filter)
            }
            GraphPattern::Slice {
                inner,
                start,
                length,
            } => {
                let inner = self.rewrite_graph_pattern(inner)?;
                inner.slice(*start, *length)
            }
            GraphPattern::Distinct { inner } => {
                let inner = self.rewrite_graph_pattern(inner)?;
                inner.distinct()
            }
            GraphPattern::OrderBy { inner, expression } => {
                let inner = self.rewrite_graph_pattern(inner)?;
                let sort_exprs = expression
                    .iter()
                    .map(|e| self.rewrite_order_expression(inner.expr_builder(), e))
                    .collect::<Result<Vec<_>, _>>()?;
                inner.order_by(&sort_exprs)
            }
            GraphPattern::Union { left, right } => {
                let lhs = self.rewrite_graph_pattern(left)?;
                let rhs = self.rewrite_graph_pattern(right)?;
                lhs.union(rhs.build()?)
            }
            GraphPattern::Graph { name, inner } => {
                let old_state = self.state.borrow().clone();
                let new_state = old_state.with_graph(GraphNamePattern::from(name.clone()));
                self.state.replace(new_state);
                let result = self.rewrite_graph_pattern(inner.as_ref());
                self.state.replace(old_state);
                result
            }
            GraphPattern::Path {
                path,
                subject,
                object,
            } => {
                let graph_name = self.state.borrow().graph.clone();
                GraphFusionLogicalPlanBuilder::new_from_property_path(
                    Arc::clone(&self.registry),
                    graph_name,
                    path.clone(),
                    subject.clone(),
                    object.clone(),
                )
            }
            GraphPattern::Minus { left, right } => {
                let left = self.rewrite_graph_pattern(left)?;
                let right = self.rewrite_graph_pattern(right)?;
                left.minus(right.build()?)
            }
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => {
                let inner = self.rewrite_graph_pattern(inner)?;

                let aggregate_exprs = aggregates
                    .iter()
                    .map(|(var, aggregate)| {
                        self.rewrite_aggregate(inner.schema(), aggregate)
                            .map(|a| (var.clone(), a))
                    })
                    .collect::<DFResult<Vec<_>>>()?;

                let aggregate_result = inner.group(variables, &aggregate_exprs)?;
                ensure_all_columns_are_rdf_terms(aggregate_result)
            }
            _ => not_impl_err!("rewrite_graph_pattern: {:?}", pattern),
        }
    }

    /// Checks whether a potential variable in the GRAPH pattern goes out of scope. This is the case
    /// if it either already is out of scope or if the variable is not projected to the outer
    /// query.
    fn graph_variable_goes_out_of_scope(&self, variables: &[Variable]) -> bool {
        let state = self.state.borrow();
        if state.graph_is_out_of_scope {
            return true;
        }

        match &state.graph {
            GraphNamePattern::Variable(v) => !variables.contains(v),
            _ => false,
        }
    }

    /// Rewrites an [Expression].
    fn rewrite_expression(
        &self,
        expr_builder: GraphFusionExprBuilder<'_>,
        expression: &Expression,
    ) -> DFResult<Expr> {
        let expression_rewriter =
            ExpressionRewriter::new(self, self.base_iri.as_ref(), expr_builder);
        expression_rewriter.rewrite(expression)
    }

    /// Rewrites an [OrderExpression].
    fn rewrite_order_expression(
        &self,
        expr_builder: GraphFusionExprBuilder<'_>,
        expression: &OrderExpression,
    ) -> DFResult<SortExpr> {
        let expression_rewriter =
            ExpressionRewriter::new(self, self.base_iri.as_ref(), expr_builder);
        let (asc, expression) = match expression {
            OrderExpression::Asc(inner) => (true, expression_rewriter.rewrite(inner)?),
            OrderExpression::Desc(inner) => (false, expression_rewriter.rewrite(inner)?),
        };
        Ok(expr_builder
            .with_encoding(expression, EncodingName::Sortable)?
            .sort(asc, true))
    }

    /// Rewrites an [AggregateExpression].
    pub fn rewrite_aggregate(
        &self,
        schema: &DFSchema,
        expression: &AggregateExpression,
    ) -> DFResult<Expr> {
        let expr_builder = GraphFusionExprBuilder::new(schema, &self.registry);
        let expression_rewriter =
            ExpressionRewriter::new(self, self.base_iri.as_ref(), expr_builder);
        match expression {
            AggregateExpression::CountSolutions { distinct } => match distinct {
                false => Ok(count(Expr::Literal(COUNT_STAR_EXPANSION))),
                true => {
                    let exprs = schema
                        .columns()
                        .into_iter()
                        .map(|c| Expr::from(Column::new_unqualified(c.name())))
                        .collect::<Vec<_>>();
                    Ok(Expr::AggregateFunction(
                        datafusion::logical_expr::expr::AggregateFunction::new_udf(
                            count_udaf(),
                            exprs,
                            true,
                            None,
                            None,
                            None,
                        ),
                    ))
                }
            },
            AggregateExpression::FunctionCall {
                name,
                expr,
                distinct,
            } => {
                let expr = expression_rewriter.rewrite(expr)?;
                let expr = expr_builder.with_encoding(expr, EncodingName::TypedValue)?;
                match name {
                    AggregateFunction::Avg => expr_builder.avg(expr, *distinct),
                    AggregateFunction::Count => expr_builder.count(expr, *distinct),
                    AggregateFunction::Max => expr_builder.max(expr),
                    AggregateFunction::Min => expr_builder.min(expr),
                    AggregateFunction::Sample => expr_builder.sample(expr),
                    AggregateFunction::Sum => expr_builder.sum(expr, *distinct),
                    AggregateFunction::GroupConcat { separator } => {
                        expr_builder.group_concat(expr, *distinct, separator.as_deref())
                    }
                    AggregateFunction::Custom(name) => {
                        plan_err!("Unsupported custom aggregate function: {name}")
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
struct RewritingState {
    /// Indicates whether the active graph is restricted to a particular pattern.
    graph: GraphNamePattern,
    /// Indicates whether the active graph pattern is out of scope.
    graph_is_out_of_scope: bool,
}

impl Default for RewritingState {
    fn default() -> Self {
        RewritingState {
            graph: GraphNamePattern::DefaultGraph,
            graph_is_out_of_scope: false,
        }
    }
}

impl RewritingState {
    /// Returns a new state like `self` but with the current graph set to `graph`.
    #[allow(clippy::unused_self)]
    fn with_graph(&self, graph: GraphNamePattern) -> RewritingState {
        RewritingState {
            graph,
            graph_is_out_of_scope: false,
        }
    }

    /// Returns a new state like `self` but where [RewritingState::graph_is_out_of_scope] is set to
    /// `true`.
    #[allow(clippy::unused_self)]
    fn with_graph_variable_going_out_of_scope(&self) -> RewritingState {
        RewritingState {
            graph_is_out_of_scope: true,
            graph: self.graph.clone(),
        }
    }
}

/// Extracts sort expressions from possible solution modifiers.
fn get_sort_expressions(graph_pattern: &GraphPattern) -> Option<&Vec<OrderExpression>> {
    match graph_pattern {
        GraphPattern::OrderBy { expression, .. } => Some(expression),
        GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner, .. }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Reduced { inner, .. } => get_sort_expressions(inner),
        _ => None,
    }
}

/// Creates the `on_expr` for a DISTINCT ON operation. This function ensures that the first
/// expressions in the results aligns with `sort_exprs`, if present.
fn create_distinct_on_expr(
    schema: &DFSchema,
    sort_exprs: Option<&Vec<OrderExpression>>,
) -> DFResult<Vec<Expr>> {
    let Some(sort_exprs) = sort_exprs else {
        return Ok(schema.columns().into_iter().map(col).collect());
    };

    let mut on_exprs = Vec::new();

    // TODO: This should be easier to do.
    let on_exprs_order = create_initial_columns_from_sort(sort_exprs)?;
    for on_expr in &on_exprs_order {
        let column = schema
            .columns()
            .into_iter()
            .find(|c| c.name() == on_expr)
            .ok_or(plan_datafusion_err!(
                "Could not find column {on_expr} in schema {schema}"
            ))?;
        on_exprs.push(column);
    }

    for column in schema.columns() {
        if !on_exprs.contains(&column) {
            on_exprs.push(column);
        }
    }

    Ok(on_exprs.into_iter().map(col).collect())
}

/// When creating a DISTINCT ON node, the initial `on_expr` expressions must match the given
/// `sort_expr` (if they exist). This function creates these initial columns from the order
/// expressions.
fn create_initial_columns_from_sort(sort_exprs: &[OrderExpression]) -> DFResult<Vec<String>> {
    sort_exprs
        .iter()
        .map(|sort_expr| match sort_expr {
            OrderExpression::Asc(Expression::Variable(var))
            | OrderExpression::Desc(Expression::Variable(var)) => Ok(var.as_str().to_owned()),
            _ => plan_err!(
                "Expression {} not supported for ORDER BY in combination with DISTINCT.",
                sort_expr
            ),
        })
        .collect::<DFResult<Vec<_>>>()
}

/// Ensures that all columns in the result are RDF terms. If not, a cast operation is inserted if
/// possible.
fn ensure_all_columns_are_rdf_terms(
    inner: GraphFusionLogicalPlanBuilder,
) -> DFResult<GraphFusionLogicalPlanBuilder> {
    // let projections = inner
    //     .schema()
    //     .fields()
    //     .into_iter()
    //     .map(|f| {
    //         let column = Expr::from(Column::new_unqualified(f.name().as_str()));
    //         if f.data_type() == &RdfTermValueEncoding::datatype() {
    //             Ok(column)
    //         } else {
    //             match f.data_type() {
    //                 DataType::Int64 => Ok(ENC_INT64_AS_RDF_TERM.call(vec![column]).alias(f.name())),
    //                 other => {
    //                     if other == &SortableTerm::data_type() {
    //                         Ok(ENC_WITH_REGULAR_ENCODING.call(vec![column]).alias(f.name()))
    //                     } else {
    //                         plan_err!("Unsupported data type {:?}", f.data_type())
    //                     }
    //                 }
    //             }
    //         }
    //     })
    //     .collect::<DFResult<Vec<_>>>()?;
    // inner.project(projections)
    todo!()
}
