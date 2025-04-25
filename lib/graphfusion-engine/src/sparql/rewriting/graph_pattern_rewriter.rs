use crate::sparql::rewriting::expression_rewriter::ExpressionRewriter;
use crate::sparql::QueryDataset;
use crate::DFResult;
use arrow_rdf::encoded::scalars::{
    encode_scalar_blank_node, encode_scalar_literal, encode_scalar_named_node, encode_scalar_null,
};
use arrow_rdf::encoded::{
    enc_group_concat, EncTerm, ENC_AVG, ENC_BOUND, ENC_COALESCE, ENC_EFFECTIVE_BOOLEAN_VALUE,
    ENC_INT64_AS_RDF_TERM, ENC_IS_COMPATIBLE, ENC_MAX, ENC_MIN, ENC_SAME_TERM, ENC_SUM,
    ENC_WITH_SORTABLE_ENCODING,
};
use arrow_rdf::sortable::{SortableTerm, ENC_WITH_REGULAR_ENCODING};
use arrow_rdf::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT, TABLE_QUADS};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{not_impl_err, plan_datafusion_err, plan_err, Column, DFSchema, JoinType};
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::functions_aggregate::count::{count, count_distinct, count_udaf};
use datafusion::functions_aggregate::first_last::first_value;
use datafusion::logical_expr::utils::COUNT_STAR_EXPANSION;
use datafusion::logical_expr::{
    lit, not, Expr, Extension, LogicalPlan, LogicalPlanBuilder, SortExpr,
};
use datafusion::prelude::{and, col};
use graphfusion_logical::paths::PathNode;
use graphfusion_logical::patterns::PatternNode;
use oxiri::Iri;
use oxrdf::{GraphName, NamedOrBlankNode, Variable};
use spargebra::algebra::{
    AggregateExpression, AggregateFunction, Expression, GraphPattern, OrderExpression,
    PropertyPathExpression,
};
use spargebra::term::{GroundTerm, NamedNodePattern, TermPattern, TriplePattern};
use std::cell::RefCell;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;

pub struct GraphPatternRewriter {
    dataset: QueryDataset,
    base_iri: Option<Iri<String>>,
    // TODO: Check if we can remove this and just use TABLE_QUADS in the logical plan
    quads_table: Arc<dyn TableProvider>,
    state: RefCell<RewritingState>,
}

impl GraphPatternRewriter {
    pub fn new(
        dataset: QueryDataset,
        base_iri: Option<Iri<String>>,
        quads_table: Arc<dyn TableProvider>,
    ) -> Self {
        Self {
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

    fn rewrite_graph_pattern(&self, pattern: &GraphPattern) -> DFResult<LogicalPlanBuilder> {
        match pattern {
            GraphPattern::Bgp { patterns } => self.rewrite_bgp(patterns),
            GraphPattern::Project { inner, variables } => {
                if self.graph_variable_goes_out_of_scope(variables) {
                    let old_state = self.state.borrow().clone();
                    let new_state = old_state.with_graph_variable_going_out_of_scope();
                    self.state.replace(new_state);
                    let result = self.rewrite_project(inner, variables);
                    self.state.replace(old_state);
                    result
                } else {
                    self.rewrite_project(inner, variables)
                }
            }
            GraphPattern::Filter { inner, expr } => self.rewrite_filter(inner, expr),
            GraphPattern::Extend {
                inner,
                expression,
                variable,
            } => self.rewrite_extend(inner, expression, variable),
            GraphPattern::Values {
                variables,
                bindings,
            } => rewrite_values(variables, bindings),
            GraphPattern::Join { left, right } => self.rewrite_join(left, right),
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => self.rewrite_left_join(left, right, expression.as_ref()),
            GraphPattern::Slice {
                inner,
                start,
                length,
            } => self.rewrite_slice(inner, *start, *length),
            GraphPattern::Distinct { inner } => self.rewrite_distinct(inner),
            GraphPattern::OrderBy { inner, expression } => self.rewrite_order_by(inner, expression),
            GraphPattern::Union { left, right } => self.rewrite_union(left, right),
            GraphPattern::Graph { name, inner } => {
                let old_state = self.state.borrow().clone();
                let new_state = old_state.with_graph(name.clone());
                self.state.replace(new_state);
                let result = self.rewrite_graph_pattern(inner.as_ref());
                self.state.replace(old_state);
                result
            }
            GraphPattern::Path {
                path,
                subject,
                object,
            } => self.rewrite_path(path, subject, object),
            GraphPattern::Minus { left, right } => self.rewrite_minus(left, right),
            GraphPattern::Group {
                inner,
                variables,
                aggregates,
            } => self.rewrite_group(inner, variables, aggregates),
            _ => not_impl_err!("rewrite_graph_pattern: {:?}", pattern),
        }
    }

    /// Rewrites a basic graph pattern into multiple scans of the quads table and joins them
    /// together.
    fn rewrite_bgp(&self, patterns: &[TriplePattern]) -> DFResult<LogicalPlanBuilder> {
        patterns
            .iter()
            .map(|p| self.rewrite_triple_pattern(p))
            .reduce(|lhs, rhs| create_join(lhs?, rhs?, JoinType::Inner, None))
            .unwrap_or_else(|| Ok(LogicalPlanBuilder::empty(true)))
    }

    /// Rewrites a single triple pattern to a scan of the QUADS table, then filtering on the graph,
    /// and lastly applying the pattern.
    ///
    /// This considers whether `pattern` is within a [GraphPattern::Graph] pattern.
    fn rewrite_triple_pattern(&self, pattern: &TriplePattern) -> DFResult<LogicalPlanBuilder> {
        let plan = LogicalPlanBuilder::scan(
            TABLE_QUADS,
            Arc::new(DefaultTableSource::new(Arc::clone(&self.quads_table))),
            None,
        )?;
        let plan = filter_by_named_graph(plan, &self.dataset, self.state.borrow().graph.as_ref())?;

        let state = self.state.borrow();
        let graph_pattern = state
            .graph
            .as_ref()
            .filter(|_| !state.graph_is_out_of_scope)
            .map(|nn| nn.clone().into_term_pattern());

        match graph_pattern {
            None => {
                let plan = plan.project([col(COL_SUBJECT), col(COL_PREDICATE), col(COL_OBJECT)])?;
                let patterns = vec![
                    pattern.subject.clone(),
                    pattern.predicate.clone().into_term_pattern(),
                    pattern.object.clone(),
                ];
                Ok(LogicalPlanBuilder::new(LogicalPlan::Extension(Extension {
                    node: Arc::new(PatternNode::try_new(plan.build()?, patterns)?),
                })))
            }
            Some(graph_pattern) => {
                let patterns = vec![
                    graph_pattern,
                    pattern.subject.clone(),
                    pattern.predicate.clone().into_term_pattern(),
                    pattern.object.clone(),
                ];
                Ok(LogicalPlanBuilder::new(LogicalPlan::Extension(Extension {
                    node: Arc::new(PatternNode::try_new(plan.build()?, patterns)?),
                })))
            }
        }
    }

    /// Rewrites a projection
    fn rewrite_project(
        &self,
        inner: &GraphPattern,
        variables: &[Variable],
    ) -> DFResult<LogicalPlanBuilder> {
        self.rewrite_graph_pattern(inner)?.project(
            variables
                .iter()
                .map(|v| col(Column::new_unqualified(v.as_str()))),
        )
    }

    /// Checks whether a potential variable in the GRAPH pattern goes out of scope. This is the case
    /// if it either already is out of scope or if the variable is not projected to the outer
    /// query.
    fn graph_variable_goes_out_of_scope(&self, variables: &[Variable]) -> bool {
        let state = self.state.borrow();
        state
            .graph
            .as_ref()
            .filter(|_| !state.graph_is_out_of_scope)
            .filter(|p| match p {
                NamedNodePattern::Variable(v) => !variables.contains(v),
                NamedNodePattern::NamedNode(_) => false,
            })
            .is_some()
    }

    /// Creates a filter node using `expression`.
    fn rewrite_filter(
        &self,
        inner: &GraphPattern,
        expression: &Expression,
    ) -> DFResult<LogicalPlanBuilder> {
        let inner = self.rewrite_graph_pattern(inner)?;
        let expression = self.rewrite_expression(inner.schema(), expression)?;
        let expression = ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![expression]);
        inner.filter(expression)
    }

    /// Creates a projection that adds another column with the name `variable`.
    ///
    /// The column is computed by evaluating `expression`.
    fn rewrite_extend(
        &self,
        inner: &GraphPattern,
        expression: &Expression,
        variable: &Variable,
    ) -> DFResult<LogicalPlanBuilder> {
        let inner = self.rewrite_graph_pattern(inner)?;

        let mut new_exprs: Vec<_> = inner
            .schema()
            .fields()
            .iter()
            .map(|f| Expr::Column(Column::new_unqualified(f.name())))
            .collect();
        new_exprs.push(
            self.rewrite_expression(inner.schema(), expression)?
                .alias(variable.as_str()),
        );

        inner.project(new_exprs)
    }

    /// Creates a logical join node for the two graph patterns.
    fn rewrite_join(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
    ) -> DFResult<LogicalPlanBuilder> {
        let left = self.rewrite_graph_pattern(left)?;
        let right = self.rewrite_graph_pattern(right)?;
        create_join(left, right, JoinType::Inner, None)
    }

    /// Creates a logical left join node for the two graph patterns. Optionally, a filter node is
    /// applied.
    fn rewrite_left_join(
        &self,
        lhs: &GraphPattern,
        rhs: &GraphPattern,
        filter: Option<&Expression>,
    ) -> DFResult<LogicalPlanBuilder> {
        let lhs = self.rewrite_graph_pattern(lhs)?;
        let rhs = self.rewrite_graph_pattern(rhs)?;

        let lhs = lhs.alias("lhs")?;
        let rhs = rhs.alias("rhs")?;
        let mut join_schema = lhs.schema().as_ref().clone();
        join_schema.merge(rhs.schema());

        let filter = filter
            .map(|f| self.rewrite_expression(&join_schema, f))
            .transpose()?;
        create_join(lhs, rhs, JoinType::Left, filter)
    }

    /// Creates a limit node that applies skip (`start`) and fetch (`length`) to `inner`.
    fn rewrite_slice(
        &self,
        inner: &GraphPattern,
        start: usize,
        length: Option<usize>,
    ) -> DFResult<LogicalPlanBuilder> {
        let inner = self.rewrite_graph_pattern(inner)?;
        LogicalPlanBuilder::limit(inner, start, length)
    }

    /// Creates a distinct node over all variables.
    fn rewrite_distinct(&self, inner: &GraphPattern) -> DFResult<LogicalPlanBuilder> {
        let sort_expr = get_sort_expressions(inner);

        let inner = self.rewrite_graph_pattern(inner)?;
        let columns = inner.schema().columns();
        let on_expr = create_distinct_on_expr(inner.schema(), sort_expr)?;
        let select_expr = columns.iter().map(|c| Expr::Column(c.clone())).collect();
        let sort_expr = sort_expr
            .map(|exprs| {
                exprs
                    .iter()
                    .map(|expr| self.rewrite_order_expression(inner.schema(), expr))
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;

        inner.distinct_on(on_expr, select_expr, sort_expr)
    }

    /// Creates a distinct node over all variables.
    fn rewrite_order_by(
        &self,
        inner: &GraphPattern,
        expression: &[OrderExpression],
    ) -> DFResult<LogicalPlanBuilder> {
        let inner = self.rewrite_graph_pattern(inner)?;
        let sort_exprs = expression
            .iter()
            .map(|e| self.rewrite_order_expression(inner.schema(), e))
            .collect::<Result<Vec<_>, _>>()?;
        LogicalPlanBuilder::sort(inner, sort_exprs)
    }

    /// Creates a union node
    fn rewrite_union(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
    ) -> DFResult<LogicalPlanBuilder> {
        let lhs = self.rewrite_graph_pattern(left)?;
        let rhs = self.rewrite_graph_pattern(right)?;

        let mut new_schema = lhs.schema().as_ref().clone();
        new_schema.merge(rhs.schema().as_ref());

        let lhs_projections = new_schema
            .columns()
            .iter()
            .map(|c| {
                if lhs.schema().has_column(c) {
                    col(c.clone())
                } else {
                    lit(encode_scalar_null()).alias(c.name())
                }
            })
            .collect::<Vec<_>>();
        let rhs_projections = new_schema
            .columns()
            .iter()
            .map(|c| {
                if rhs.schema().has_column(c) {
                    col(c.clone())
                } else {
                    lit(encode_scalar_null()).alias(c.name())
                }
            })
            .collect::<Vec<_>>();

        lhs.project(lhs_projections)?
            .union(rhs.project(rhs_projections)?.build()?)
    }

    /// Rewrites a path to a [PathNode].
    fn rewrite_path(
        &self,
        path: &PropertyPathExpression,
        subject: &TermPattern,
        object: &TermPattern,
    ) -> DFResult<LogicalPlanBuilder> {
        let node = PathNode::new(
            self.state.borrow().graph.clone(),
            subject.clone(),
            path.clone(),
            object.clone(),
        )?;
        Ok(LogicalPlanBuilder::new(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        })))
    }

    /// Rewrites a MINUS pattern to an except expression.
    fn rewrite_minus(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
    ) -> DFResult<LogicalPlanBuilder> {
        let lhs = self.rewrite_graph_pattern(left)?;
        let rhs = self.rewrite_graph_pattern(right)?;

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

        let overlapping_keys = lhs_keys
            .intersection(&rhs_keys)
            .collect::<HashSet<&String>>();
        if overlapping_keys.is_empty() {
            return Ok(lhs);
        }

        let lhs = lhs.alias("lhs")?;
        let rhs = rhs.alias("rhs")?;

        let mut join_filters = Vec::new();

        for k in &overlapping_keys {
            let expr = ENC_IS_COMPATIBLE.call(vec![
                Expr::from(Column::new(Some("lhs"), *k)),
                Expr::from(Column::new(Some("rhs"), *k)),
            ]);
            join_filters.push(expr);
        }
        let any_both_not_null = overlapping_keys
            .iter()
            .map(|k| {
                and(
                    Expr::from(Column::new(Some("lhs"), *k)).is_not_null(),
                    Expr::from(Column::new(Some("rhs"), *k)).is_not_null(),
                )
            })
            .reduce(Expr::or)
            .ok_or(plan_datafusion_err!(
                "There must be at least one overlapping key"
            ))?;
        join_filters.push(any_both_not_null);

        let filter_expr = join_filters.into_iter().reduce(Expr::and);

        let projections = lhs_keys
            .iter()
            .map(|k| Expr::from(Column::new(Some("lhs"), k)).alias(k))
            .collect::<Vec<_>>();

        lhs.join_detailed(
            rhs.build()?,
            JoinType::LeftAnti,
            (Vec::<Column>::new(), Vec::<Column>::new()),
            filter_expr,
            false,
        )?
        .project(projections)
    }

    /// Rewrites a GROUP pattern to a group plan.
    fn rewrite_group(
        &self,
        inner: &GraphPattern,
        variables: &[Variable],
        aggregates: &[(Variable, AggregateExpression)],
    ) -> DFResult<LogicalPlanBuilder> {
        let inner = self.rewrite_graph_pattern(inner)?;
        let expression_rewriter =
            ExpressionRewriter::new(self, self.base_iri.as_ref(), inner.schema());

        let group_exprs = variables
            .iter()
            .map(|var| {
                expression_rewriter
                    .rewrite(&Expression::Variable(var.clone()))
                    .map(|e| ENC_WITH_SORTABLE_ENCODING.call(vec![e]).alias(var.as_str()))
            })
            .collect::<DFResult<Vec<_>>>()?;
        let aggregate_exprs = aggregates
            .iter()
            .map(|(var, aggregate)| {
                self.rewrite_aggregate(inner.schema(), aggregate)
                    .map(|a| a.alias(var.as_str()))
            })
            .collect::<DFResult<Vec<_>>>()?;

        let aggregate_result = inner.aggregate(group_exprs, aggregate_exprs)?;
        ensure_all_columns_are_rdf_terms(aggregate_result)
    }

    /// Rewrites an [Expression].
    fn rewrite_expression(&self, schema: &DFSchema, expression: &Expression) -> DFResult<Expr> {
        let expression_rewriter = ExpressionRewriter::new(self, self.base_iri.as_ref(), schema);
        expression_rewriter.rewrite(expression)
    }

    /// Rewrites an [OrderExpression].
    fn rewrite_order_expression(
        &self,
        schema: &DFSchema,
        expression: &OrderExpression,
    ) -> DFResult<SortExpr> {
        let expression_rewriter = ExpressionRewriter::new(self, self.base_iri.as_ref(), schema);
        let (asc, expression) = match expression {
            OrderExpression::Asc(inner) => (true, expression_rewriter.rewrite(inner)?),
            OrderExpression::Desc(inner) => (false, expression_rewriter.rewrite(inner)?),
        };
        Ok(ENC_WITH_SORTABLE_ENCODING
            .call(vec![expression])
            .sort(asc, true))
    }

    /// Rewrites an [AggregateExpression].
    pub fn rewrite_aggregate(
        &self,
        schema: &DFSchema,
        expression: &AggregateExpression,
    ) -> DFResult<Expr> {
        let expression_rewriter = ExpressionRewriter::new(self, self.base_iri.as_ref(), schema);
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
                let result = match name {
                    AggregateFunction::Avg => Expr::AggregateFunction(
                        datafusion::logical_expr::expr::AggregateFunction::new_udf(
                            Arc::new(ENC_AVG.deref().clone()),
                            vec![expr],
                            *distinct,
                            None,
                            None,
                            None,
                        ),
                    ),
                    AggregateFunction::Count => match distinct {
                        false => count(expr),
                        true => count_distinct(expr),
                    },
                    AggregateFunction::Max => Expr::AggregateFunction(
                        datafusion::logical_expr::expr::AggregateFunction::new_udf(
                            Arc::new(ENC_MAX.deref().clone()),
                            vec![expr],
                            false, // DISTINCT doesn't change the result.
                            None,
                            None,
                            None,
                        ),
                    ),
                    AggregateFunction::Min => Expr::AggregateFunction(
                        datafusion::logical_expr::expr::AggregateFunction::new_udf(
                            Arc::new(ENC_MIN.deref().clone()),
                            vec![expr],
                            false, // DISTINCT doesn't change the result.
                            None,
                            None,
                            None,
                        ),
                    ),
                    AggregateFunction::Sample => first_value(expr, None),
                    AggregateFunction::Sum => Expr::AggregateFunction(
                        datafusion::logical_expr::expr::AggregateFunction::new_udf(
                            Arc::new(ENC_SUM.deref().clone()),
                            vec![expr],
                            *distinct,
                            None,
                            None,
                            None,
                        ),
                    ),
                    AggregateFunction::GroupConcat { separator } => Expr::AggregateFunction(
                        datafusion::logical_expr::expr::AggregateFunction::new_udf(
                            Arc::new(enc_group_concat(
                                separator.as_ref().map_or(" ", |s| s.as_str()),
                            )),
                            vec![expr],
                            *distinct,
                            None,
                            None,
                            None,
                        ),
                    ),
                    AggregateFunction::Custom(name) => {
                        return plan_err!("Unsupported custom aggregate function: {name}");
                    }
                };
                Ok(result)
            }
        }
    }
}

#[derive(Clone, Default)]
struct RewritingState {
    /// Indicates whether the active graph is restricted to a particular pattern.
    /// Note that [RewritingState::only_named_graphs] is still necessary, as the pattern can go
    /// out of scope.
    graph: Option<NamedNodePattern>,
    /// Indicates whether the active graph pattern is out of scope.
    graph_is_out_of_scope: bool,
}

impl RewritingState {
    /// Returns a new state like `self` but with the current graph set to `graph`.
    #[allow(clippy::unused_self)]
    fn with_graph(&self, graph: NamedNodePattern) -> RewritingState {
        RewritingState {
            graph: Some(graph),
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

/// Creates a logical node that holds the given VALUES as encoded RDF terms
fn rewrite_values(
    variables: &[Variable],
    bindings: &[Vec<Option<GroundTerm>>],
) -> DFResult<LogicalPlanBuilder> {
    if bindings.is_empty() {
        return Ok(LogicalPlanBuilder::empty(true));
    }

    let values = bindings
        .iter()
        .map(|solution| encode_solution(solution))
        .collect::<DFResult<Vec<_>>>()?;
    let values = LogicalPlanBuilder::values(values)?;

    let projections: Vec<_> = values
        .schema()
        .columns()
        .iter()
        .zip(variables.iter())
        .map(|(column, variable)| Expr::from(column.clone()).alias(variable.as_str()))
        .collect();

    values.project(projections)
}

fn encode_solution(terms: &[Option<GroundTerm>]) -> DFResult<Vec<Expr>> {
    terms
        .iter()
        .map(|t| {
            Ok(match t {
                Some(GroundTerm::NamedNode(nn)) => lit(encode_scalar_named_node(nn.as_ref())),
                Some(GroundTerm::Literal(literal)) => lit(encode_scalar_literal(literal.as_ref())?),
                None => lit(encode_scalar_null()),
                #[allow(clippy::unimplemented, reason = "Not production ready")]
                _ => unimplemented!("encoding values"),
            })
        })
        .collect()
}

/// Creates a join node of two logical plans that contain encoded RDF Terms.
///
/// See https://www.w3.org/TR/sparql11-query/#defn_algCompatibleMapping for a definition for
/// compatible mappings.
fn create_join(
    lhs: LogicalPlanBuilder,
    rhs: LogicalPlanBuilder,
    join_type: JoinType,
    filter: Option<Expr>,
) -> DFResult<LogicalPlanBuilder> {
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
            ENC_IS_COMPATIBLE.call(vec![
                Expr::from(Column::new(Some("lhs"), k)),
                Expr::from(Column::new(Some("rhs"), k)),
            ])
        })
        .collect::<Vec<_>>();
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
        let filter = ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![filter]);
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
    lhs_keys: &HashSet<String>,
    rhs_keys: &HashSet<String>,
    variable: &str,
) -> Expr {
    let lhs_expr = Expr::from(Column::new(Some("lhs"), variable));
    let rhs_expr = Expr::from(Column::new(Some("rhs"), variable));

    match (lhs_keys.contains(variable), rhs_keys.contains(variable)) {
        (true, true) => ENC_COALESCE.call(vec![lhs_expr, rhs_expr]),
        (true, false) => lhs_expr,
        (false, true) => rhs_expr,
        (false, false) => lit(encode_scalar_null()),
    }
    .alias(variable)
}

/// Adds filter operations that constraints the solutions of patterns to named graphs if necessary.
fn filter_by_named_graph(
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
            let graph_filter = ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![ENC_SAME_TERM.call(vec![
                col(COL_GRAPH),
                lit(encode_scalar_named_node(nn.as_ref())),
            ])]);
            plan.filter(graph_filter)
        }
    }
}

fn create_filter_for_default_graph(graph: Option<&[GraphName]>) -> Expr {
    let Some(graph) = graph else {
        return not(ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![ENC_BOUND.call(vec![col(COL_GRAPH)])]));
    };

    graph
        .iter()
        .map(|name| match name {
            GraphName::NamedNode(nn) => {
                ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![ENC_SAME_TERM.call(vec![
                    col(COL_GRAPH),
                    lit(encode_scalar_named_node(nn.as_ref())),
                ])])
            }
            GraphName::BlankNode(bnode) => ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![ENC_SAME_TERM
                .call(vec![
                    col(COL_GRAPH),
                    lit(encode_scalar_blank_node(bnode.as_ref())),
                ])]),
            GraphName::DefaultGraph => {
                not(ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![ENC_BOUND.call(vec![col(COL_GRAPH)])]))
            }
        })
        .reduce(Expr::or)
        .unwrap_or(lit(false))
}

fn create_filter_for_named_graph(graphs: Option<&[NamedOrBlankNode]>) -> Expr {
    let Some(graphs) = graphs else {
        return ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![ENC_BOUND.call(vec![col(COL_GRAPH)])]);
    };

    graphs
        .iter()
        .map(|name| match name {
            NamedOrBlankNode::NamedNode(nn) => {
                ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![ENC_SAME_TERM.call(vec![
                    col(COL_GRAPH),
                    lit(encode_scalar_named_node(nn.as_ref())),
                ])])
            }
            NamedOrBlankNode::BlankNode(bnode) => {
                ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![ENC_SAME_TERM.call(vec![
                    col(COL_GRAPH),
                    lit(encode_scalar_blank_node(bnode.as_ref())),
                ])])
            }
        })
        .reduce(Expr::or)
        .unwrap_or(lit(false))
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
        return Ok(schema
            .columns()
            .iter()
            .map(|c| {
                ENC_WITH_SORTABLE_ENCODING
                    .call(vec![Expr::Column(c.clone())])
                    .alias(c.name())
            })
            .collect());
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

    Ok(on_exprs
        .into_iter()
        .map(|c| ENC_WITH_SORTABLE_ENCODING.call(vec![Expr::Column(c)]))
        .collect())
}

/// When creating a DISTINCT ON node, the initial `on_expr` expressions must match the given
/// `sort_expr` (if they exist). This function creates these initial columns from the order
/// expressions.
fn create_initial_columns_from_sort(sort_exprs: &[OrderExpression]) -> DFResult<Vec<String>> {
    sort_exprs
        .iter()
        .map(|sort_expr| match sort_expr.expression() {
            Expression::Variable(var) => Ok(var.as_str().to_owned()),
            _ => plan_err!(
                "Expression {} not supported for ORDER BY in combination with DISTINCT.",
                sort_expr
            ),
        })
        .collect::<DFResult<Vec<_>>>()
}

/// Ensures that all columns in the result are RDF terms. If not, a cast operation is inserted if
/// possible.
fn ensure_all_columns_are_rdf_terms(inner: LogicalPlanBuilder) -> DFResult<LogicalPlanBuilder> {
    let projections = inner
        .schema()
        .fields()
        .into_iter()
        .map(|f| {
            let column = Expr::from(Column::new_unqualified(f.name().as_str()));
            if f.data_type() == &EncTerm::data_type() {
                Ok(column)
            } else {
                match f.data_type() {
                    DataType::Int64 => Ok(ENC_INT64_AS_RDF_TERM.call(vec![column]).alias(f.name())),
                    other => {
                        if other == &SortableTerm::data_type() {
                            Ok(ENC_WITH_REGULAR_ENCODING.call(vec![column]).alias(f.name()))
                        } else {
                            plan_err!("Unsupported data type {:?}", f.data_type())
                        }
                    }
                }
            }
        })
        .collect::<DFResult<Vec<_>>>()?;
    inner.project(projections)
}
