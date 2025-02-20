use crate::results::decode_rdf_terms;
use crate::DFResult;
use arrow_rdf::encoded::scalars::{
    encode_scalar_blank_node, encode_scalar_literal, encode_scalar_named_node,
};
use arrow_rdf::encoded::{EncTerm, EncTermField, ENC_AS_NATIVE_BOOLEAN, ENC_AS_RDF_TERM_SORT, ENC_EFFECTIVE_BOOLEAN_VALUE, ENC_EQ, ENC_GREATER_OR_EQUAL, ENC_GREATER_THAN, ENC_IS_IRI, ENC_LESS_OR_EQUAL, ENC_LESS_THAN, ENC_NOT, ENC_SAME_TERM, ENC_STR};
use arrow_rdf::{COL_OBJECT, COL_PREDICATE, COL_SUBJECT, TABLE_QUADS};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{
    not_impl_err, plan_err, Column, DFSchema, DFSchemaRef, JoinType, ScalarValue,
};
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::logical_expr::{lit, Expr, LogicalPlan, LogicalPlanBuilder, SortExpr};
use datafusion::prelude::col;
use oxrdf::Variable;
use spargebra::algebra::{Expression, Function, GraphPattern, OrderExpression};
use spargebra::term::{GroundTerm, TermPattern, TriplePattern};
use std::collections::HashSet;
use std::sync::Arc;

pub struct GraphPatternRewriter {
    // TODO: Check if we can remove this and just use TABLE_QUADS in the logical plan
    quads_table: Arc<dyn TableProvider>,
}

impl GraphPatternRewriter {
    pub fn new(quads_table: Arc<dyn TableProvider>) -> Self {
        Self { quads_table }
    }

    pub fn rewrite(&self, pattern: &GraphPattern) -> DFResult<LogicalPlan> {
        let plan = self.rewrite_graph_pattern(pattern)?;
        Ok(decode_rdf_terms(plan.build()?)?)
    }

    fn rewrite_graph_pattern(&self, pattern: &GraphPattern) -> DFResult<LogicalPlanBuilder> {
        match pattern {
            GraphPattern::Bgp { patterns } => self.rewrite_bgp(patterns),
            GraphPattern::Project { inner, variables } => self.rewrite_project(inner, variables),
            GraphPattern::Filter { inner, expr } => self.rewrite_filter(inner, expr),
            GraphPattern::Extend {
                inner,
                expression,
                variable,
            } => self.rewrite_extend(inner, expression, variable),
            GraphPattern::Values {
                variables,
                bindings,
            } => self.rewrite_values(variables, bindings),
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
            pattern => not_impl_err!("rewrite_graph_pattern: {:?}", pattern),
        }
    }

    /// Rewrites a basic graph pattern into multiple scans of the quads table and joins them
    /// together.
    fn rewrite_bgp(&self, patterns: &Vec<TriplePattern>) -> DFResult<LogicalPlanBuilder> {
        patterns
            .iter()
            .map(|p| self.rewrite_triple_pattern(p))
            .reduce(|lhs, rhs| create_join(lhs?, rhs?, JoinType::Inner))
            .unwrap_or_else(|| {
                Ok(LogicalPlanBuilder::scan(
                    TABLE_QUADS,
                    Arc::new(DefaultTableSource::new(Arc::clone(&self.quads_table))),
                    None,
                )?)
            })
    }

    fn rewrite_triple_pattern(&self, pattern: &TriplePattern) -> DFResult<LogicalPlanBuilder> {
        let plan = LogicalPlanBuilder::scan(
            TABLE_QUADS,
            Arc::new(DefaultTableSource::new(Arc::clone(&self.quads_table))),
            None,
        )?;

        let (subject_filter, subject_projection) =
            pattern_to_filter_and_projections(&pattern.subject)?;
        let predicate_term_pattern = pattern.predicate.clone().into_term_pattern();
        let (predicate_filter, predicate_projection) =
            pattern_to_filter_and_projections(&predicate_term_pattern)?;
        let (object_filter, object_projection) =
            pattern_to_filter_and_projections(&pattern.object)?;

        let plan = filter_equal_to_scalar(plan, COL_SUBJECT, subject_filter)?;
        let plan = filter_equal_to_scalar(plan, COL_PREDICATE, predicate_filter)?;
        let plan = filter_equal_to_scalar(plan, COL_OBJECT, object_filter)?;

        let projections = [
            (COL_SUBJECT, subject_projection),
            (COL_PREDICATE, predicate_projection),
            (COL_OBJECT, object_projection),
        ]
        .into_iter()
        .filter_map(|(col_name, var)| var.map(|new_col_name| col(col_name).alias(new_col_name)));

        plan.project(projections)
    }

    fn rewrite_project(
        &self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
    ) -> DFResult<LogicalPlanBuilder> {
        self.rewrite_graph_pattern(inner)?
            .project(variables.iter().map(|v| col(v.as_str())))
    }

    /// Creates a filter node using `expression`.
    fn rewrite_filter(
        &self,
        inner: &GraphPattern,
        expression: &Expression,
    ) -> DFResult<LogicalPlanBuilder> {
        self.rewrite_graph_pattern(inner)?
            .filter(ENC_AS_NATIVE_BOOLEAN.call(vec![
                ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![self.rewrite_expr(expression)?]),
            ]))
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
            .map(|f| Expr::Column(Column::from(f.name())))
            .collect();
        new_exprs.push(self.rewrite_expr(expression)?.alias(variable.as_str()));

        inner.project(new_exprs)
    }

    /// Creates a logical node that holds the given VALUES as encoded RDF terms
    fn rewrite_values(
        &self,
        variables: &Vec<Variable>,
        bindings: &Vec<Vec<Option<GroundTerm>>>,
    ) -> DFResult<LogicalPlanBuilder> {
        if bindings.is_empty() {
            return Ok(LogicalPlanBuilder::empty(false));
        }

        let fields: Vec<_> = variables
            .iter()
            .map(|v| Field::new(v.as_str(), EncTerm::term_type(), true))
            .collect();
        let schema = DFSchemaRef::new(DFSchema::try_from(Schema::new(fields))?);

        let values = bindings
            .iter()
            .map(|solution| encode_solution(solution))
            .collect::<DFResult<Vec<_>>>()?;

        LogicalPlanBuilder::values_with_schema(values, &schema)
    }

    /// Creates a logical join node for the two graph patterns.
    fn rewrite_join(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
    ) -> DFResult<LogicalPlanBuilder> {
        let left = self.rewrite_graph_pattern(left)?;
        let right = self.rewrite_graph_pattern(right)?;
        create_join(left, right, JoinType::Inner)
    }

    /// Creates a logical left join node for the two graph patterns. Optionally, a filter node is
    /// applied.
    fn rewrite_left_join(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
        filter: Option<&Expression>,
    ) -> DFResult<LogicalPlanBuilder> {
        let left = self.rewrite_graph_pattern(left)?;
        let right = self.rewrite_graph_pattern(right)?;

        if let Some(filter) = filter {
            create_join(left, right, JoinType::Left)?.filter(ENC_AS_NATIVE_BOOLEAN.call(vec![
                ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![self.rewrite_expr(filter)?]),
            ]))
        } else {
            create_join(left, right, JoinType::Left)
        }
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
        // TODO: Does this use SAME Term?
        self.rewrite_graph_pattern(inner)?.distinct()
    }

    /// Creates a distinct node over all variables.
    fn rewrite_order_by(
        &self,
        inner: &GraphPattern,
        expression: &Vec<OrderExpression>,
    ) -> DFResult<LogicalPlanBuilder> {
        let inner = self.rewrite_graph_pattern(inner)?;
        let sort_exprs = expression
            .iter()
            .map(|e| self.rewrite_order_expr(e))
            .collect::<Result<Vec<_>, _>>()?;
        LogicalPlanBuilder::sort(inner, sort_exprs)
    }

    /// Creates a union node
    fn rewrite_union(
        &self,
        left: &GraphPattern,
        right: &GraphPattern,
    ) -> DFResult<LogicalPlanBuilder> {
        let left = self.rewrite_graph_pattern(left)?;
        let right = self.rewrite_graph_pattern(right)?;
        left.union(right.build()?)
    }

    //
    // Expressions
    //

    /// Rewrites an [Expression].
    fn rewrite_expr(&self, expression: &Expression) -> DFResult<Expr> {
        match expression {
            Expression::Not(inner) => Ok(ENC_NOT.call(vec![
                ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![self.rewrite_expr(inner)?])
            ])),
            Expression::Equal(lhs, rhs) => {
                Ok(ENC_EQ.call(vec![self.rewrite_expr(lhs)?, self.rewrite_expr(rhs)?]))
            }
            Expression::SameTerm(lhs, rhs) => {
                Ok(ENC_SAME_TERM.call(vec![self.rewrite_expr(lhs)?, self.rewrite_expr(rhs)?]))
            }
            Expression::Greater(lhs, rhs) => {
                Ok(ENC_GREATER_THAN.call(vec![self.rewrite_expr(lhs)?, self.rewrite_expr(rhs)?]))
            }
            Expression::GreaterOrEqual(lhs, rhs) => {
                Ok(ENC_GREATER_OR_EQUAL
                    .call(vec![self.rewrite_expr(lhs)?, self.rewrite_expr(rhs)?]))
            }
            Expression::Less(lhs, rhs) => {
                Ok(ENC_LESS_THAN.call(vec![self.rewrite_expr(lhs)?, self.rewrite_expr(rhs)?]))
            }
            Expression::LessOrEqual(lhs, rhs) => {
                Ok(ENC_LESS_OR_EQUAL.call(vec![self.rewrite_expr(lhs)?, self.rewrite_expr(rhs)?]))
            }
            Expression::Literal(literal) => {
                Ok(Expr::Literal(encode_scalar_literal(literal.as_ref())?))
            }
            Expression::Variable(var) => Ok(Expr::Column(Column::from(var.as_str()))),
            Expression::FunctionCall(function, args) => self.rewrite_function_call(function, args),
            expr => not_impl_err!("{:?}", expr),
        }
    }

    /// Rewrites a SPARQL function call to a Scalar UDF call
    fn rewrite_function_call(&self, function: &Function, args: &Vec<Expression>) -> DFResult<Expr> {
        match function {
            // Functions on RDF Terms
            Function::IsIri => {
                assert_eq!(args.len(), 1);
                Ok(ENC_IS_IRI.call(vec![self.rewrite_expr(&args[0])?]))
            }
            Function::Str => {
                assert_eq!(args.len(), 1);
                Ok(ENC_STR.call(vec![self.rewrite_expr(&args[0])?]))
            }
            _ => not_impl_err!("rewrite_function_call: {:?}", function),
        }
    }

    /// Rewrites an [OrderExpression].
    fn rewrite_order_expr(&self, expression: &OrderExpression) -> DFResult<SortExpr> {
        let (asc, expression) = match expression {
            OrderExpression::Asc(inner) => (true, self.rewrite_expr(inner)?),
            OrderExpression::Desc(inner) => (false, self.rewrite_expr(inner)?),
        };
        Ok(ENC_AS_RDF_TERM_SORT.call(vec![expression]).sort(asc, true))
    }
}

fn pattern_to_filter_and_projections(
    pattern: &TermPattern,
) -> DFResult<(Option<ScalarValue>, Option<&str>)> {
    Ok(match pattern {
        TermPattern::NamedNode(nn) => (Some(encode_scalar_named_node(nn.as_ref())), None),
        TermPattern::BlankNode(bnode) => (
            Some(encode_scalar_blank_node(bnode.as_ref())),
            Some(bnode.as_str()),
        ),
        TermPattern::Literal(lit) => (Some(encode_scalar_literal(lit.as_ref())?), None),
        TermPattern::Variable(var) => (None, Some(var.as_str())),
        TermPattern::Triple(_) => unimplemented!(),
    })
}

fn encode_solution(terms: &Vec<Option<GroundTerm>>) -> DFResult<Vec<Expr>> {
    terms
        .iter()
        .map(|t| {
            Ok(match t {
                Some(GroundTerm::NamedNode(nn)) => {
                    Expr::Literal(encode_scalar_named_node(nn.as_ref()))
                }
                Some(GroundTerm::Literal(lit)) => {
                    Expr::Literal(encode_scalar_literal(lit.as_ref())?)
                }
                None => Expr::Literal(ScalarValue::Null),
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
) -> DFResult<LogicalPlanBuilder> {
    let lhs = lhs.alias("lhs")?;
    let rhs = rhs.alias("rhs")?;
    let lhs_keys: HashSet<_> = lhs.schema().field_names().iter().cloned().collect();
    let rhs_keys: HashSet<_> = rhs.schema().field_names().iter().cloned().collect();
    let join_on_exprs = lhs_keys.intersection(&rhs_keys).map(|k| {
        ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![ENC_SAME_TERM.call(vec![
            col(String::from("lhs.") + k),
            col(String::from("rhs.") + k),
        ])])
    });
    lhs.join_on(rhs.build()?, join_type, join_on_exprs)
}

/// Creates a filter node that applies the predicate
fn filter_equal_to_scalar(
    plan: LogicalPlanBuilder,
    col_name: &str,
    filter: Option<ScalarValue>,
) -> DFResult<LogicalPlanBuilder> {
    let Some(filter) = filter else {
        return Ok(plan);
    };

    if filter.data_type() != EncTerm::term_type() {
        return plan_err!("Unexpected type of scalar in filter_equal_to_scalar");
    };

    let ScalarValue::Union(Some((type_id, _)), _, _) = &filter else {
        return plan_err!("Unexpected value of scalar in filter_equal_to_scalar");
    };

    if *type_id == EncTermField::BlankNode.type_id() {
        return Ok(plan);
    }

    plan.filter(ENC_AS_NATIVE_BOOLEAN.call(vec![ENC_EQ.call(vec![col(col_name), lit(filter)])]))
}
