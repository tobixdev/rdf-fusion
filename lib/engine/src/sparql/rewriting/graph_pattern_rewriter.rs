use crate::sparql::rewriting::expression_rewriter::ExpressionRewriter;
use crate::sparql::QueryDataset;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{not_impl_err, plan_err, Column, DFSchema};
use datafusion::functions_aggregate::count::{count, count_udaf};
use datafusion::logical_expr::utils::COUNT_STAR_EXPANSION;
use datafusion::logical_expr::{Expr, LogicalPlan, SortExpr};
use rdf_fusion_encoding::EncodingName;
use rdf_fusion_functions::registry::{RdfFusionFunctionRegistry, RdfFusionFunctionRegistryRef};
use rdf_fusion_logical::join::SparqlJoinType;
use rdf_fusion_logical::{ActiveGraph, RdfFusionExprBuilderRoot, RdfFusionLogicalPlanBuilder};
use rdf_fusion_model::Iri;
use rdf_fusion_model::{GraphName, Variable};
use spargebra::algebra::{
    AggregateExpression, AggregateFunction, Expression, GraphPattern, OrderExpression,
};
use spargebra::term::NamedNodePattern;
use std::cell::RefCell;
use std::sync::Arc;

/// TODO
pub struct GraphPatternRewriter {
    /// TODO
    registry: RdfFusionFunctionRegistryRef,
    /// TODO
    dataset: QueryDataset,
    /// TODO
    base_iri: Option<Iri<String>>,
    /// TODO
    state: RefCell<RewritingState>,
}

impl GraphPatternRewriter {
    /// TODO
    pub fn new(
        registry: RdfFusionFunctionRegistryRef,
        dataset: QueryDataset, // TODO: Moving dataset and base_iri to rewrite allows reusing
        base_iri: Option<Iri<String>>,
    ) -> Self {
        let active_graph = compute_default_active_graph(&dataset);
        let state = RewritingState::default().with_active_graph(active_graph);
        Self {
            registry,
            dataset,
            base_iri,
            state: RefCell::new(state),
        }
    }

    /// TODO
    pub fn registry(&self) -> &dyn RdfFusionFunctionRegistry {
        self.registry.as_ref()
    }

    /// TODO
    pub fn rewrite(&self, pattern: &GraphPattern) -> DFResult<LogicalPlan> {
        let plan = self.rewrite_graph_pattern(pattern)?;
        plan.with_plain_terms()?.build()
    }

    /// TODO
    fn rewrite_graph_pattern(
        &self,
        pattern: &GraphPattern,
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        match pattern {
            GraphPattern::Bgp { patterns } => {
                let state = self.state.borrow();
                RdfFusionLogicalPlanBuilder::new_from_bgp(
                    Arc::clone(&self.registry),
                    &state.active_graph,
                    state.graph_name_var.as_ref(),
                    patterns,
                )
            }
            GraphPattern::Project { inner, variables } => {
                if self.graph_variable_goes_out_of_scope(variables) {
                    let old_state = self.state.borrow().clone();
                    let new_state = old_state.without_graph_variable();
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
                let expr = self.rewrite_expression(inner.schema(), expr)?;
                inner.filter(expr)
            }
            GraphPattern::Extend {
                inner,
                expression,
                variable,
            } => {
                let inner = self.rewrite_graph_pattern(inner)?;
                let expr = self.rewrite_expression(inner.schema(), expression)?;
                inner.extend(variable.clone(), expr)
            }
            GraphPattern::Values {
                variables,
                bindings,
            } => RdfFusionLogicalPlanBuilder::new_from_values(
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

                let filter = expression
                    .as_ref()
                    .map(|f| self.rewrite_expression(&join_schema, f))
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
                let sort_exprs = get_sort_expressions(inner);
                let inner = self.rewrite_graph_pattern(inner)?;

                let Some(sort_exprs) = sort_exprs else {
                    return inner.distinct();
                };

                let sort_exprs = sort_exprs
                    .iter()
                    .map(|e| self.rewrite_order_expression(inner.schema(), e))
                    .collect::<Result<Vec<_>, _>>()?;
                inner.distinct_with_sort(sort_exprs)
            }
            GraphPattern::OrderBy { inner, expression } => {
                let inner = self.rewrite_graph_pattern(inner)?;
                let sort_exprs = expression
                    .iter()
                    .map(|e| self.rewrite_order_expression(inner.schema(), e))
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
                let active_graph = compute_active_graph_for_pattern(&self.dataset, name);
                let variable = match name {
                    NamedNodePattern::Variable(var) => Some(var.clone()),
                    _ => None,
                };
                let new_state = old_state
                    .with_active_graph(active_graph)
                    .with_graph_variable(variable);
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
                let state = self.state.borrow();
                RdfFusionLogicalPlanBuilder::new_from_property_path(
                    Arc::clone(&self.registry),
                    state.active_graph.clone(),
                    state.graph_name_var.clone(),
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
        match &state.graph_name_var {
            Some(v) => !variables.contains(v),
            _ => false,
        }
    }

    /// Rewrites an [Expression].
    fn rewrite_expression(&self, schema: &DFSchema, expression: &Expression) -> DFResult<Expr> {
        let expr_builder_root = RdfFusionExprBuilderRoot::new(self.registry.as_ref(), schema);
        let expression_rewriter =
            ExpressionRewriter::new(self, expr_builder_root, self.base_iri.as_ref());
        expression_rewriter.rewrite(expression)
    }

    /// Rewrites an [OrderExpression].
    fn rewrite_order_expression(
        &self,
        schema: &DFSchema,
        expression: &OrderExpression,
    ) -> DFResult<SortExpr> {
        let expr_builder_root = RdfFusionExprBuilderRoot::new(self.registry.as_ref(), schema);
        let expression_rewriter =
            ExpressionRewriter::new(self, expr_builder_root, self.base_iri.as_ref());
        let (asc, expression) = match expression {
            OrderExpression::Asc(inner) => (true, expression_rewriter.rewrite(inner)?),
            OrderExpression::Desc(inner) => (false, expression_rewriter.rewrite(inner)?),
        };
        Ok(expr_builder_root
            .create_builder(expression)
            .with_encoding(EncodingName::Sortable)?
            .build()?
            .sort(asc, true))
    }

    /// Rewrites an [AggregateExpression].
    pub fn rewrite_aggregate(
        &self,
        schema: &DFSchema,
        expression: &AggregateExpression,
    ) -> DFResult<Expr> {
        let expr_builder_root = RdfFusionExprBuilderRoot::new(self.registry.as_ref(), schema);
        let expression_rewriter =
            ExpressionRewriter::new(self, expr_builder_root, self.base_iri.as_ref());
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
                let expr = expr_builder_root
                    .create_builder(expr)
                    .with_encoding(EncodingName::TypedValue)?;
                Ok(match name {
                    AggregateFunction::Avg => expr.avg(*distinct),
                    AggregateFunction::Count => expr.count(*distinct),
                    AggregateFunction::Max => expr.max(),
                    AggregateFunction::Min => expr.min(),
                    AggregateFunction::Sample => expr.sample(),
                    AggregateFunction::Sum => expr.sum(*distinct),
                    AggregateFunction::GroupConcat { separator } => {
                        expr.group_concat(*distinct, separator.as_deref())
                    }
                    AggregateFunction::Custom(name) => {
                        plan_err!("Unsupported custom aggregate function: {name}")
                    }
                }?
                .build_any())
            }
        }
    }
}

#[derive(Clone)]
struct RewritingState {
    /// Currently active graph.
    active_graph: ActiveGraph,
    /// Indicates whether the graph should be bound to a variable.
    graph_name_var: Option<Variable>,
}

impl Default for RewritingState {
    fn default() -> Self {
        RewritingState {
            active_graph: ActiveGraph::DefaultGraph,
            graph_name_var: None,
        }
    }
}

impl RewritingState {
    /// TODO
    #[allow(clippy::unused_self)]
    fn with_graph_variable(&self, variable: Option<Variable>) -> RewritingState {
        RewritingState {
            graph_name_var: variable,
            active_graph: self.active_graph.clone(),
        }
    }

    /// TODO
    #[allow(clippy::unused_self)]
    fn without_graph_variable(&self) -> RewritingState {
        RewritingState {
            graph_name_var: None,
            active_graph: self.active_graph.clone(),
        }
    }

    /// TODO
    #[allow(clippy::unused_self)]
    fn with_active_graph(&self, active_graph: ActiveGraph) -> RewritingState {
        RewritingState {
            graph_name_var: None,
            active_graph,
        }
    }
}

fn compute_default_active_graph(dataset: &QueryDataset) -> ActiveGraph {
    match dataset.default_graph_graphs() {
        None => ActiveGraph::DefaultGraph,
        Some(graphs) => ActiveGraph::Union(graphs.iter().cloned().collect()),
    }
}

fn compute_active_graph_for_pattern(
    dataset: &QueryDataset,
    name: &NamedNodePattern,
) -> ActiveGraph {
    match name {
        NamedNodePattern::NamedNode(nn) => {
            ActiveGraph::Union(vec![GraphName::NamedNode(nn.clone())])
        }
        NamedNodePattern::Variable(_) => match dataset.available_named_graphs() {
            None => ActiveGraph::AnyNamedGraph,
            Some(graphs) => ActiveGraph::Union(graphs.iter().cloned().map(Into::into).collect()),
        },
    }
}

/// Ensures that all columns in the result are RDF terms. If not, a cast operation is inserted if
/// possible.
fn ensure_all_columns_are_rdf_terms(
    inner: RdfFusionLogicalPlanBuilder,
) -> DFResult<RdfFusionLogicalPlanBuilder> {
    let projections = inner
        .schema()
        .fields()
        .into_iter()
        .map(|f| {
            let column = Expr::from(Column::new_unqualified(f.name().as_str()));
            let encoding = EncodingName::try_from_data_type(f.data_type());
            if matches!(
                encoding,
                Some(EncodingName::TypedValue) | Some(EncodingName::PlainTerm)
            ) {
                Ok(column)
            } else {
                let expr_builder = inner.expr_builder(column);
                match f.data_type() {
                    DataType::Int64 => Ok(expr_builder.native_int64_as_term()?.alias(f.name())),
                    other => plan_err!("Unsupported data type {:?}", other),
                }
            }
        })
        .collect::<DFResult<Vec<_>>>()?;

    let registry = Arc::clone(inner.registry());
    let new_plan = inner.into_inner().project(projections)?;
    Ok(RdfFusionLogicalPlanBuilder::new(
        Arc::new(new_plan.build()?),
        registry,
    ))
}

/// Extracts sort expressions from possible solution modifiers.
fn get_sort_expressions(graph_pattern: &GraphPattern) -> Option<&Vec<OrderExpression>> {
    match graph_pattern {
        GraphPattern::OrderBy { expression, .. } => Some(expression),
        GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner, .. }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Reduced { inner, .. }
        | GraphPattern::Group { inner, .. } => get_sort_expressions(inner),
        _ => None,
    }
}
