use crate::sparql::paths::PathNode;
use crate::sparql::QueryDataset;
use crate::DFResult;
use arrow_rdf::encoded::scalars::{
    encode_scalar_blank_node, encode_scalar_literal, encode_scalar_named_node, encode_scalar_null,
};
use arrow_rdf::encoded::{enc_iri, EncTerm, EncTermField, ENC_ABS, ENC_ADD, ENC_AND, ENC_AS_BOOLEAN, ENC_AS_DATETIME, ENC_AS_DECIMAL, ENC_AS_DOUBLE, ENC_AS_FLOAT, ENC_AS_INT, ENC_AS_INTEGER, ENC_AS_NATIVE_BOOLEAN, ENC_AS_STRING, ENC_BNODE_NULLARY, ENC_BNODE_UNARY, ENC_BOOLEAN_AS_RDF_TERM, ENC_BOUND, ENC_CEIL, ENC_CONTAINS, ENC_DATATYPE, ENC_DIV, ENC_EFFECTIVE_BOOLEAN_VALUE, ENC_ENCODEFORURI, ENC_EQ, ENC_FLOOR, ENC_GREATER_OR_EQUAL, ENC_GREATER_THAN, ENC_IS_BLANK, ENC_IS_COMPATIBLE, ENC_IS_IRI, ENC_IS_LITERAL, ENC_IS_NUMERIC, ENC_LANG, ENC_LANGMATCHES, ENC_LCASE, ENC_LESS_OR_EQUAL, ENC_LESS_THAN, ENC_MUL, ENC_OR, ENC_RAND, ENC_REGEX_BINARY, ENC_REGEX_TERNARY, ENC_REPLACE_QUATERNARY, ENC_REPLACE_TERNARY, ENC_ROUND, ENC_SAME_TERM, ENC_STR, ENC_STRAFTER, ENC_STRBEFORE, ENC_STRDT, ENC_STRENDS, ENC_STRLANG, ENC_STRLEN, ENC_STRSTARTS, ENC_STRUUID, ENC_SUB, ENC_SUBSTR, ENC_UCASE, ENC_UNARY_MINUS, ENC_UNARY_PLUS, ENC_UUID};
use arrow_rdf::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT, TABLE_QUADS};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{
    internal_err, not_impl_err, plan_err, Column, DFSchema, DFSchemaRef, JoinType, ScalarValue,
};
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::logical_expr::{
    lit, not, Expr, Extension, LogicalPlan, LogicalPlanBuilder, Operator, ScalarUDF, SortExpr,
};
use datafusion::prelude::col;
use oxiri::Iri;
use oxrdf::vocab::xsd;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Variable};
use spargebra::algebra::{
    Expression, Function, GraphPattern, OrderExpression, PropertyPathExpression,
};
use spargebra::term::{GroundTerm, NamedNodePattern, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct GraphPatternRewriter {
    dataset: QueryDataset,
    base_iri: Option<Iri<String>>,
    // TODO: Check if we can remove this and just use TABLE_QUADS in the logical plan
    quads_table: Arc<dyn TableProvider>,
    state: RewritingState,
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
            state: RewritingState::default(),
        }
    }

    pub fn rewrite(&mut self, pattern: &GraphPattern) -> DFResult<LogicalPlan> {
        let plan = self.rewrite_graph_pattern(pattern)?;
        Ok(plan.build()?)
    }

    fn rewrite_graph_pattern(&mut self, pattern: &GraphPattern) -> DFResult<LogicalPlanBuilder> {
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
            GraphPattern::Graph { name, inner } => {
                let old = self.state.clone();
                self.state = old.with_graph(name.clone());
                let result = self.rewrite_graph_pattern(inner.as_ref());
                self.state = old;
                result
            }
            GraphPattern::Path {
                path,
                subject,
                object,
            } => self.rewrite_path(path, subject, object),
            pattern => not_impl_err!("rewrite_graph_pattern: {:?}", pattern),
        }
    }

    /// Rewrites a basic graph pattern into multiple scans of the quads table and joins them
    /// together.
    fn rewrite_bgp(&mut self, patterns: &Vec<TriplePattern>) -> DFResult<LogicalPlanBuilder> {
        patterns
            .iter()
            .map(|p| self.rewrite_triple_pattern(p))
            .reduce(|lhs, rhs| create_join(lhs?, rhs?, JoinType::Inner, None))
            .unwrap_or_else(|| {
                Ok(LogicalPlanBuilder::scan(
                    TABLE_QUADS,
                    Arc::new(DefaultTableSource::new(Arc::clone(&mut self.quads_table))),
                    None,
                )?)
            })
    }

    /// Rewrites a single triple pattern to a SELECT on the QUADS table.
    ///
    /// This considers whether `pattern` is within a [GraphPattern::Graph] pattern.
    fn rewrite_triple_pattern(&mut self, pattern: &TriplePattern) -> DFResult<LogicalPlanBuilder> {
        let plan = LogicalPlanBuilder::scan(
            TABLE_QUADS,
            Arc::new(DefaultTableSource::new(Arc::clone(&mut self.quads_table))),
            None,
        )?;

        let graph_pattern = self.state.graph.as_ref();
        let plan = filter_by_triple_part(plan, pattern)?;
        let plan = filter_by_named_graph(plan, &self.dataset, graph_pattern)?;
        let plan = filter_pattern_same_variables(plan, graph_pattern, pattern)?;
        let plan = project_quads_to_variables(plan, graph_pattern, pattern)?;

        Ok(plan)
    }

    /// Rewrites a projection
    fn rewrite_project(
        &mut self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
    ) -> DFResult<LogicalPlanBuilder> {
        self.rewrite_graph_pattern(inner)?.project(
            variables
                .iter()
                .map(|v| col(Column::new_unqualified(v.as_str()))),
        )
    }

    /// Creates a filter node using `expression`.
    fn rewrite_filter(
        &mut self,
        inner: &GraphPattern,
        expression: &Expression,
    ) -> DFResult<LogicalPlanBuilder> {
        let inner = self.rewrite_graph_pattern(inner)?;
        let expression = self.rewrite_expr(expression)?;
        let expression = create_filter_expression(inner.schema(), expression)?;
        inner.filter(expression)
    }

    /// Creates a projection that adds another column with the name `variable`.
    ///
    /// The column is computed by evaluating `expression`.
    fn rewrite_extend(
        &mut self,
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
        new_exprs.push(self.rewrite_expr(expression)?.alias(variable.as_str()));

        inner.project(new_exprs)
    }

    /// Creates a logical node that holds the given VALUES as encoded RDF terms
    fn rewrite_values(
        &mut self,
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
        &mut self,
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
        &mut self,
        lhs: &GraphPattern,
        rhs: &GraphPattern,
        filter: Option<&Expression>,
    ) -> DFResult<LogicalPlanBuilder> {
        let lhs = self.rewrite_graph_pattern(lhs)?;
        let rhs = self.rewrite_graph_pattern(rhs)?;
        let filter = filter.map(|f| self.rewrite_expr(f)).transpose()?;
        create_join(lhs, rhs, JoinType::Left, filter)
    }

    /// Creates a limit node that applies skip (`start`) and fetch (`length`) to `inner`.
    fn rewrite_slice(
        &mut self,
        inner: &GraphPattern,
        start: usize,
        length: Option<usize>,
    ) -> DFResult<LogicalPlanBuilder> {
        let inner = self.rewrite_graph_pattern(inner)?;
        LogicalPlanBuilder::limit(inner, start, length)
    }

    /// Creates a distinct node over all variables.
    fn rewrite_distinct(&mut self, _inner: &GraphPattern) -> DFResult<LogicalPlanBuilder> {
        plan_err!("Distinct not yet implemented")
    }

    /// Creates a distinct node over all variables.
    fn rewrite_order_by(
        &mut self,
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
        &mut self,
        left: &GraphPattern,
        right: &GraphPattern,
    ) -> DFResult<LogicalPlanBuilder> {
        let lhs = self.rewrite_graph_pattern(left)?;
        let rhs = self.rewrite_graph_pattern(right)?;

        let mut new_schema = lhs.schema().as_ref().clone();
        new_schema.merge(&rhs.schema().as_ref());

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
        &mut self,
        path: &PropertyPathExpression,
        subject: &TermPattern,
        object: &TermPattern,
    ) -> DFResult<LogicalPlanBuilder> {
        let node = PathNode::new(
            self.state.graph.clone(),
            subject.clone(),
            path.clone(),
            object.clone(),
        )?;
        Ok(LogicalPlanBuilder::new(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        })))
    }

    //
    // Expressions
    //

    /// Rewrites an [Expression].
    fn rewrite_expr(&mut self, expression: &Expression) -> DFResult<Expr> {
        match expression {
            Expression::Bound(var) => {
                Ok(ENC_BOUND.call(vec![Expr::from(Column::new_unqualified(var.as_str()))]))
            }
            Expression::Not(inner) => Ok(ENC_BOOLEAN_AS_RDF_TERM.call(vec![Expr::Not(Box::new(
                ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![self.rewrite_expr(inner)?]),
            ))])),
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
            Expression::Variable(var) => Ok(Expr::Column(Column::new_unqualified(var.as_str()))),
            Expression::FunctionCall(function, args) => self.rewrite_function_call(function, args),
            Expression::NamedNode(nn) => Ok(Expr::Literal(encode_scalar_named_node(nn.as_ref()))),
            Expression::Or(lhs, rhs) => logical_expression(self, Operator::Or, lhs, rhs),
            Expression::And(lhs, rhs) => logical_expression(self, Operator::And, lhs, rhs),
            Expression::In(_, _) => unimplemented!("Expression::In"),
            Expression::Add(lhs, rhs) => binary_udf(self, &ENC_ADD, lhs, rhs),
            Expression::Subtract(lhs, rhs) => binary_udf(self, &ENC_SUB, lhs, rhs),
            Expression::Multiply(lhs, rhs) => binary_udf(self, &ENC_MUL, lhs, rhs),
            Expression::Divide(lhs, rhs) => binary_udf(self, &ENC_DIV, lhs, rhs),
            Expression::UnaryPlus(value) => unary_udf(self, &ENC_UNARY_PLUS, value),
            Expression::UnaryMinus(value) => unary_udf(self, &ENC_UNARY_MINUS, value),
            Expression::Exists(_) => unimplemented!("Expression::Exists"),
            Expression::If(_, _, _) => unimplemented!("Expression::If"),
            Expression::Coalesce(_) => unimplemented!("Expression::Coalesce"),
        }
    }

    /// Rewrites a SPARQL function call
    fn rewrite_function_call(
        &mut self,
        function: &Function,
        args: &Vec<Expression>,
    ) -> DFResult<Expr> {
        let args = args
            .iter()
            .map(|e| self.rewrite_expr(e))
            .collect::<DFResult<Vec<_>>>()?;
        match function {
            // Functions on RDF Terms
            Function::IsIri => Ok(ENC_IS_IRI.call(args)),
            Function::IsBlank => Ok(ENC_IS_BLANK.call(args)),
            Function::IsLiteral => Ok(ENC_IS_LITERAL.call(args)),
            Function::IsNumeric => Ok(ENC_IS_NUMERIC.call(args)),
            Function::Str => Ok(ENC_STR.call(args)),
            Function::Lang => Ok(ENC_LANG.call(args)),
            Function::Datatype => Ok(ENC_DATATYPE.call(args)),
            Function::Iri => Ok(enc_iri(self.base_iri.clone()).call(args)),
            Function::BNode => match args.len() {
                0 => Ok(ENC_BNODE_NULLARY.call(args)),
                1 => Ok(ENC_BNODE_UNARY.call(args)),
                _ => internal_err!("Unexpected arity for BNode"),
            },
            Function::StrDt => Ok(ENC_STRDT.call(args)),
            Function::StrLang => Ok(ENC_STRLANG.call(args)),
            Function::Uuid => Ok(ENC_UUID.call(args)),
            Function::StrUuid => Ok(ENC_STRUUID.call(args)),
            // Strings
            Function::StrLen => Ok(ENC_STRLEN.call(args)),
            Function::SubStr => Ok(ENC_SUBSTR.call(args)),
            Function::UCase => Ok(ENC_UCASE.call(args)),
            Function::LCase => Ok(ENC_LCASE.call(args)),
            Function::StrStarts => Ok(ENC_STRSTARTS.call(args)),
            Function::StrEnds => Ok(ENC_STRENDS.call(args)),
            Function::Contains => Ok(ENC_CONTAINS.call(args)),
            Function::StrBefore => Ok(ENC_STRBEFORE.call(args)),
            Function::StrAfter => Ok(ENC_STRAFTER.call(args)),
            Function::EncodeForUri => Ok(ENC_ENCODEFORURI.call(args)),
            Function::Concat => not_impl_err!("Function::Concat"),
            Function::LangMatches => Ok(ENC_LANGMATCHES.call(args)),
            Function::Regex => Ok(match args.len() {
                2 => ENC_REGEX_BINARY.call(args),
                3 => ENC_REGEX_TERNARY.call(args),
                _ => unreachable!("Unexpected number of args"),
            }),
            Function::Replace => Ok(match args.len() {
                3 => ENC_REPLACE_TERNARY.call(args),
                4 => ENC_REPLACE_QUATERNARY.call(args),
                _ => unreachable!("Unexpected number of args"),
            }),
            // Numeric
            Function::Abs => Ok(ENC_ABS.call(args)),
            Function::Round => Ok(ENC_ROUND.call(args)),
            Function::Ceil => Ok(ENC_CEIL.call(args)),
            Function::Floor => Ok(ENC_FLOOR.call(args)),
            Function::Rand => Ok(ENC_RAND.call(args)),
            // Custom
            Function::Custom(nn) => self.rewrite_custom_function_call(nn, args),
            _ => not_impl_err!("rewrite_function_call: {:?}", function),
        }
    }

    /// Rewrites a custom SPARQL function call
    fn rewrite_custom_function_call(
        &mut self,
        function: &NamedNode,
        args: Vec<Expr>,
    ) -> DFResult<Expr> {
        let supported_conversion_functions = HashMap::from([
            (xsd::BOOLEAN.as_str(), ENC_AS_BOOLEAN),
            (xsd::INT.as_str(), ENC_AS_INT),
            (xsd::INTEGER.as_str(), ENC_AS_INTEGER),
            (xsd::FLOAT.as_str(), ENC_AS_FLOAT),
            (xsd::DOUBLE.as_str(), ENC_AS_DOUBLE),
            (xsd::DECIMAL.as_str(), ENC_AS_DECIMAL),
            (xsd::DATE_TIME.as_str(), ENC_AS_DATETIME),
            (xsd::STRING.as_str(), ENC_AS_STRING),
        ]);

        let supported_conversion = supported_conversion_functions.get(function.as_str());
        if let Some(supported_conversion) = supported_conversion {
            if args.len() != 1 {
                return plan_err!(
                    "Unsupported argument count for conversion function {}.",
                    function.as_str()
                );
            }
            return Ok(supported_conversion.call(args));
        }

        plan_err!("Custom Function {} is not supported.", function.as_str())
    }

    /// Rewrites an [OrderExpression].
    fn rewrite_order_expr(&mut self, expression: &OrderExpression) -> DFResult<SortExpr> {
        let (_asc, _expression) = match expression {
            OrderExpression::Asc(inner) => (true, self.rewrite_expr(inner)?),
            OrderExpression::Desc(inner) => (false, self.rewrite_expr(inner)?),
        };
        plan_err!("Sorting not yet implemented")
    }
}

#[derive(Clone, Default)]
struct RewritingState {
    graph: Option<NamedNodePattern>,
}

impl RewritingState {
    fn with_graph(&self, graph: NamedNodePattern) -> RewritingState {
        RewritingState {
            graph: Some(graph),
            ..*self
        }
    }
}

fn logical_expression(
    rewriter: &mut GraphPatternRewriter,
    operator: Operator,
    lhs: &Box<Expression>,
    rhs: &Box<Expression>,
) -> DFResult<Expr> {
    let lhs = ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![rewriter.rewrite_expr(lhs)?]);
    let rhs = ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![rewriter.rewrite_expr(rhs)?]);

    let connective_impl = match operator {
        Operator::And => ENC_AND,
        Operator::Or => ENC_OR,
        _ => plan_err!("Unsupported logical expression: {}", &operator)?,
    };
    let booleans = connective_impl.call(vec![lhs, rhs]);
    Ok(ENC_BOOLEAN_AS_RDF_TERM.call(vec![booleans]))
}

fn unary_udf(
    rewriter: &mut GraphPatternRewriter,
    udf: &ScalarUDF,
    value: &Box<Expression>,
) -> DFResult<Expr> {
    let value = rewriter.rewrite_expr(value)?;
    Ok(udf.call(vec![value]))
}

fn binary_udf(
    rewriter: &mut GraphPatternRewriter,
    udf: &ScalarUDF,
    lhs: &Box<Expression>,
    rhs: &Box<Expression>,
) -> DFResult<Expr> {
    let lhs = rewriter.rewrite_expr(lhs)?;
    let rhs = rewriter.rewrite_expr(rhs)?;
    Ok(udf.call(vec![lhs, rhs]))
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

fn create_filter_expression(schema: &DFSchema, filter: Expr) -> DFResult<Expr> {
    let new_expr = filter.transform(|e| {
        Ok(match e {
            Expr::Column(c) if !schema.has_column(&c) => {
                Transformed::yes(lit(encode_scalar_null()))
            }
            e => Transformed::no(e),
        })
    })?;
    Ok(ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![new_expr.data]))
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
        .map(|c| c.name().to_string())
        .collect();
    let rhs_keys: HashSet<_> = rhs
        .schema()
        .columns()
        .into_iter()
        .map(|c| c.name().to_string())
        .collect();

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
                    Expr::Column(c) => Transformed::yes(use_lhs_or_rhs(&lhs_keys, c.name())),
                    _ => Transformed::no(e),
                })
            })?
            .data;
        let filter = create_filter_expression(&join_schema, filter)?;
        join_filters.push(filter);
    }
    let filter_expr = join_filters.into_iter().reduce(Expr::and);

    let projections = lhs_keys
        .union(&rhs_keys)
        .map(|k| use_lhs_or_rhs(&lhs_keys, k))
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

fn use_lhs_or_rhs(lhs_keys: &HashSet<String>, k: &str) -> Expr {
    if lhs_keys.contains(k) {
        Expr::from(Column::new(Some("lhs"), k))
    } else {
        Expr::from(Column::new(Some("rhs"), k))
    }
        .alias(k)
}

/// Adds filter operations that constraints the solutions of patterns that use literals.
///
/// For example, for the pattern `?a foaf:knows ?b` this functions adds a filter that ensures that
/// the predicate is `foaf:knows`.
fn filter_by_triple_part(
    plan: LogicalPlanBuilder,
    pattern: &TriplePattern,
) -> DFResult<LogicalPlanBuilder> {
    let subject_filter = pattern_to_filter_scalar(&pattern.subject)?;
    let predicate_filter =
        pattern_to_filter_scalar(&pattern.predicate.clone().into_term_pattern())?;
    let object_filter = pattern_to_filter_scalar(&pattern.object)?;

    let plan = filter_equal_to_scalar(plan, COL_SUBJECT, subject_filter)?;
    let plan = filter_equal_to_scalar(plan, COL_PREDICATE, predicate_filter)?;
    let plan = filter_equal_to_scalar(plan, COL_OBJECT, object_filter)?;

    Ok(plan)
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

/// Adds filter operations that constraints the solutions of patterns that use the same variable
/// twice.
///
/// For example, for the pattern `?a ?a ?b` this functions adds a constraint that ensures that the
/// subject is equal to the predicate.
fn filter_pattern_same_variables(
    plan: LogicalPlanBuilder,
    graph_pattern: Option<&NamedNodePattern>,
    pattern: &TriplePattern,
) -> DFResult<LogicalPlanBuilder> {
    let graph_variable = graph_pattern
        .map(|p| p.clone().into_term_pattern())
        .and_then(|p| pattern_to_variable_name(&p));
    let subject_variable = pattern_to_variable_name(&pattern.subject);
    let predicate_variable =
        pattern_to_variable_name(&pattern.predicate.clone().into_term_pattern());
    let object_variable = pattern_to_variable_name(&pattern.object);
    let all_variables = [
        (graph_variable, COL_GRAPH),
        (subject_variable, COL_SUBJECT),
        (predicate_variable, COL_PREDICATE),
        (object_variable, COL_OBJECT),
    ];

    let mut mappings = HashMap::new();
    for (variable, quad_column) in all_variables {
        match variable {
            Some(variable) => {
                if !mappings.contains_key(&variable) {
                    mappings.insert(variable.clone(), Vec::new());
                }
                mappings.get_mut(&variable).unwrap().push(quad_column);
            }
            None => {}
        }
    }

    let mut result_plan = plan;
    for value in mappings.into_values() {
        let columns = value
            .into_iter()
            .map(|v| col(Column::new_unqualified(v)))
            .collect::<Vec<_>>();
        let constraint = columns
            .iter()
            .zip(columns.iter().skip(1))
            .map(|(a, b)| {
                ENC_EFFECTIVE_BOOLEAN_VALUE
                    .call(vec![ENC_SAME_TERM.call(vec![a.clone(), b.clone()])])
            })
            .reduce(|a, b| a.and(b));
        result_plan = match constraint {
            Some(constraint) => result_plan.filter(constraint)?,
            _ => result_plan,
        }
    }

    Ok(result_plan)
}

fn project_quads_to_variables(
    plan: LogicalPlanBuilder,
    graph_pattern: Option<&NamedNodePattern>,
    pattern: &TriplePattern,
) -> DFResult<LogicalPlanBuilder> {
    let graph_pattern =
        graph_pattern.and_then(|p| pattern_to_variable_name(&p.clone().into_term_pattern()));
    let predicate_pattern =
        pattern_to_variable_name(&pattern.predicate.clone().into_term_pattern());
    let possible_projections = [
        (COL_GRAPH, graph_pattern),
        (COL_SUBJECT, pattern_to_variable_name(&pattern.subject)),
        (COL_PREDICATE, predicate_pattern),
        (COL_OBJECT, pattern_to_variable_name(&pattern.object)),
    ];

    let mut already_projected = HashSet::new();
    let mut projections = Vec::new();
    for (old_name, new_name) in possible_projections {
        match &new_name {
            Some(new_name) if !already_projected.contains(new_name) => {
                let expr = col(Column::new_unqualified(old_name)).alias(new_name);
                already_projected.insert(new_name.clone());
                projections.push(expr);
            }
            _ => {}
        }
    }

    plan.project(projections)
}

fn pattern_to_variable_name(pattern: &TermPattern) -> Option<String> {
    match pattern {
        TermPattern::BlankNode(bnode) => Some(bnode.as_ref().as_str().into()),
        TermPattern::Variable(var) => Some(var.as_str().into()),
        _ => None,
    }
}

fn pattern_to_filter_scalar(pattern: &TermPattern) -> DFResult<Option<ScalarValue>> {
    Ok(match pattern {
        TermPattern::NamedNode(nn) => Some(encode_scalar_named_node(nn.as_ref())),
        TermPattern::BlankNode(bnode) => Some(encode_scalar_blank_node(bnode.as_ref())),
        TermPattern::Literal(lit) => Some(encode_scalar_literal(lit.as_ref())?),
        TermPattern::Variable(_) => None,
        TermPattern::Triple(_) => unimplemented!(),
    })
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

    plan.filter(ENC_AS_NATIVE_BOOLEAN.call(vec![
        ENC_SAME_TERM.call(vec![col(Column::new_unqualified(col_name)), lit(filter)]),
    ]))
}
