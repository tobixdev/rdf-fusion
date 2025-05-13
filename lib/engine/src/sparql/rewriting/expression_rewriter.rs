use crate::sparql::rewriting::GraphPatternRewriter;
use crate::DFResult;
use datafusion::common::{internal_err, plan_datafusion_err, plan_err, Column, Spans};
use datafusion::functions_aggregate::count::count;
use datafusion::logical_expr::utils::COUNT_STAR_EXPANSION;
use datafusion::logical_expr::{lit, or, Expr, LogicalPlanBuilder, Operator, Subquery};
use datafusion::prelude::{and, exists};
use rdf_fusion_logical::RdfFusionExprBuilder;
use rdf_fusion_model::vocab::xsd;
use rdf_fusion_model::Iri;
use rdf_fusion_model::{DateTime, TermRef};
use rdf_fusion_model::{Literal, NamedNode};
use spargebra::algebra::{Expression, Function, GraphPattern};
use std::collections::HashSet;
use std::sync::Arc;

pub(super) struct ExpressionRewriter<'rewriter> {
    graph_rewriter: &'rewriter GraphPatternRewriter,
    base_iri: Option<&'rewriter Iri<String>>,
    expr_builder: RdfFusionExprBuilder<'rewriter>,
}

impl<'rewriter> ExpressionRewriter<'rewriter> {
    /// Creates a new expression rewriter for a given schema.
    pub fn new(
        graph_rewriter: &'rewriter GraphPatternRewriter,
        base_iri: Option<&'rewriter Iri<String>>,
        expr_builder: RdfFusionExprBuilder<'rewriter>,
    ) -> Self {
        Self {
            graph_rewriter,
            base_iri,
            expr_builder,
        }
    }

    /// Rewrites an [Expression].
    pub fn rewrite(&self, expression: &Expression) -> DFResult<Expr> {
        match expression {
            Expression::Bound(var) => {
                let var = self.rewrite(&Expression::Variable(var.clone()))?;
                self.expr_builder.bound(var)
            }
            Expression::Not(inner) => self.expr_builder.not(self.rewrite(inner)?),
            Expression::Equal(lhs, rhs) => self
                .expr_builder
                .equal(self.rewrite(lhs)?, self.rewrite(rhs)?),
            Expression::SameTerm(lhs, rhs) => self
                .expr_builder
                .same_term(self.rewrite(lhs)?, self.rewrite(rhs)?),
            Expression::Greater(lhs, rhs) => self
                .expr_builder
                .greater_than(self.rewrite(lhs)?, self.rewrite(rhs)?),
            Expression::GreaterOrEqual(lhs, rhs) => self
                .expr_builder
                .greater_or_equal(self.rewrite(lhs)?, self.rewrite(rhs)?),
            Expression::Less(lhs, rhs) => self
                .expr_builder
                .less_than(self.rewrite(lhs)?, self.rewrite(rhs)?),
            Expression::LessOrEqual(lhs, rhs) => self
                .expr_builder
                .less_or_equal(self.rewrite(lhs)?, self.rewrite(rhs)?),
            Expression::Literal(literal) => {
                self.expr_builder.literal(TermRef::from(literal.as_ref()))
            }
            Expression::Variable(var) => self.expr_builder.variable(var.as_ref()),
            Expression::FunctionCall(function, args) => self.rewrite_function_call(function, args),
            Expression::NamedNode(nn) => self.expr_builder.literal(TermRef::from(nn.as_ref())),
            Expression::Or(lhs, rhs) => {
                self.expr_builder.or(self.rewrite(lhs)?, self.rewrite(rhs)?)
            }
            Expression::And(lhs, rhs) => logical_expression(self, Operator::And, lhs, rhs),
            Expression::In(lhs, rhs) => self.rewrite_in(lhs, rhs),
            Expression::Add(lhs, rhs) => self
                .expr_builder
                .add(self.rewrite(lhs)?, self.rewrite(rhs)?),
            Expression::Subtract(lhs, rhs) => self
                .expr_builder
                .sub(self.rewrite(lhs)?, self.rewrite(rhs)?),
            Expression::Multiply(lhs, rhs) => self
                .expr_builder
                .mul(self.rewrite(lhs)?, self.rewrite(rhs)?),
            Expression::Divide(lhs, rhs) => self
                .expr_builder
                .div(self.rewrite(lhs)?, self.rewrite(rhs)?),
            Expression::UnaryPlus(value) => self.expr_builder.unary_plus(self.rewrite(value)?),
            Expression::UnaryMinus(value) => self.expr_builder.unary_minus(self.rewrite(value)?),
            Expression::Exists(pattern) => self.rewrite_exists(pattern),
            Expression::If(test, if_true, if_false) => self.rewrite_if(test, if_true, if_false),
            Expression::Coalesce(args) => {
                let args = args
                    .iter()
                    .map(|arg| self.rewrite(arg))
                    .collect::<DFResult<Vec<_>>>()?;
                self.expr_builder.coalesce(args)
            }
        }
    }

    /// Rewrites a SPARQL function call.
    ///
    /// We assume here that the length of `args` matches the expected number of arguments.
    fn rewrite_function_call(&self, function: &Function, args: &[Expression]) -> DFResult<Expr> {
        let args = args
            .iter()
            .map(|e| self.rewrite(e))
            .collect::<DFResult<Vec<_>>>()?;
        match function {
            // Functions on RDF Terms
            Function::IsIri => {
                let arg = unary_args(args)?;
                self.expr_builder.is_iri(arg)
            }
            Function::IsBlank => {
                let arg = unary_args(args)?;
                self.expr_builder.is_blank(arg)
            }
            Function::IsLiteral => {
                let arg = unary_args(args)?;
                self.expr_builder.is_literal(arg)
            }
            Function::IsNumeric => {
                let arg = unary_args(args)?;
                self.expr_builder.is_numeric(arg)
            }
            Function::Str => {
                let arg = unary_args(args)?;
                self.expr_builder.str(arg)
            }
            Function::Lang => {
                let arg = unary_args(args)?;
                self.expr_builder.lang(arg)
            }
            Function::Datatype => {
                let arg = unary_args(args)?;
                self.expr_builder.datatype(arg)
            }
            Function::Iri => {
                let arg = unary_args(args)?;
                self.expr_builder.iri(self.base_iri, arg)
            }
            Function::BNode => match args.len() {
                0 => self.expr_builder.bnode(),
                1 => {
                    let arg = unary_args(args)?;
                    self.expr_builder.bnode_from(arg)
                }
                _ => internal_err!("Unexpected arity for BNode"),
            },
            Function::StrDt => {
                let (lhs, rhs) = binary_args(args)?;
                self.expr_builder.str_dt(lhs, rhs)
            }
            Function::StrLang => {
                let arg = unary_args(args)?;
                self.expr_builder.str_lang(arg)
            }
            Function::Uuid => self.expr_builder.uuid(),
            Function::StrUuid => self.expr_builder.str_uuid(),
            // Strings
            Function::StrLen => {
                let arg = unary_args(args)?;
                self.expr_builder.str_len(arg)
            }
            Function::SubStr => match args.len() {
                2 => {
                    let (lhs, rhs) = binary_args(args)?;
                    self.expr_builder.substr(lhs, rhs)
                }
                3 => {
                    let (arg0, arg1, arg2) = ternary_args(args)?;
                    self.expr_builder.substr_with_length(arg0, arg1, arg2)
                }
                _ => unreachable!("Unexpected number of args"),
            },
            Function::UCase => {
                let arg = unary_args(args)?;
                self.expr_builder.ucase(arg)
            }
            Function::LCase => {
                let arg = unary_args(args)?;
                self.expr_builder.lcase(arg)
            }
            Function::StrStarts => {
                let (lhs, rhs) = binary_args(args)?;
                self.expr_builder.str_starts(lhs, rhs)
            }
            Function::StrEnds => {
                let (lhs, rhs) = binary_args(args)?;
                self.expr_builder.str_ends(lhs, rhs)
            }
            Function::Contains => {
                let (lhs, rhs) = binary_args(args)?;
                self.expr_builder.contains(lhs, rhs)
            }
            Function::StrBefore => {
                let (lhs, rhs) = binary_args(args)?;
                self.expr_builder.str_before(lhs, rhs)
            }
            Function::StrAfter => {
                let (lhs, rhs) = binary_args(args)?;
                self.expr_builder.str_after(lhs, rhs)
            }
            Function::EncodeForUri => {
                let arg = unary_args(args)?;
                self.expr_builder.encode_for_uri(arg)
            }
            Function::Concat => self.expr_builder.concat(args),
            Function::LangMatches => {
                let (lhs, rhs) = binary_args(args)?;
                self.expr_builder.lang_matches(lhs, rhs)
            }
            Function::Regex => match args.len() {
                2 => {
                    let (lhs, rhs) = binary_args(args)?;
                    self.expr_builder.regex(lhs, rhs)
                }
                3 => {
                    let (arg0, arg1, arg2) = ternary_args(args)?;
                    self.expr_builder.regex_with_flags(arg0, arg1, arg2)
                }
                _ => unreachable!("Unexpected number of args"),
            },
            Function::Replace => match args.len() {
                3 => {
                    let (arg0, arg1, arg2) = ternary_args(args)?;
                    self.expr_builder.replace(arg0, arg1, arg2)
                }
                4 => {
                    let (arg0, arg1, arg2, arg3) = quarternary_args(args)?;
                    self.expr_builder.replace_with_flags(arg0, arg1, arg2, arg3)
                }
                _ => unreachable!("Unexpected number of args"),
            },
            // Numeric
            Function::Abs => {
                let arg = unary_args(args)?;
                self.expr_builder.abs(arg)
            }
            Function::Round => {
                let arg = unary_args(args)?;
                self.expr_builder.round(arg)
            }
            Function::Ceil => {
                let arg = unary_args(args)?;
                self.expr_builder.ceil(arg)
            }
            Function::Floor => {
                let arg = unary_args(args)?;
                self.expr_builder.floor(arg)
            }
            Function::Rand => self.expr_builder.rand(),
            // Dates & Durations
            Function::Year => {
                let arg = unary_args(args)?;
                self.expr_builder.year(arg)
            }
            Function::Month => {
                let arg = unary_args(args)?;
                self.expr_builder.month(arg)
            }
            Function::Day => {
                let arg = unary_args(args)?;
                self.expr_builder.day(arg)
            }
            Function::Hours => {
                let arg = unary_args(args)?;
                self.expr_builder.hours(arg)
            }
            Function::Minutes => {
                let arg = unary_args(args)?;
                self.expr_builder.minutes(arg)
            }
            Function::Seconds => {
                let arg = unary_args(args)?;
                self.expr_builder.seconds(arg)
            }
            Function::Timezone => {
                let arg = unary_args(args)?;
                self.expr_builder.timezone(arg)
            }
            Function::Tz => {
                let arg = unary_args(args)?;
                self.expr_builder.tz(arg)
            }
            Function::Now => {
                let literal =
                    Literal::new_typed_literal(DateTime::now().to_string(), xsd::DATE_TIME);
                self.expr_builder.literal(TermRef::from(literal.as_ref()))
            }
            // Hashing
            Function::Md5 => {
                let arg = unary_args(args)?;
                self.expr_builder.md5(arg)
            }
            Function::Sha1 => {
                let arg = unary_args(args)?;
                self.expr_builder.sha1(arg)
            }
            Function::Sha256 => {
                let arg = unary_args(args)?;
                self.expr_builder.sha256(arg)
            }
            Function::Sha384 => {
                let arg = unary_args(args)?;
                self.expr_builder.sha384(arg)
            }
            Function::Sha512 => {
                let arg = unary_args(args)?;
                self.expr_builder.sha512(arg)
            }
            // Custom
            Function::Custom(nn) => self.rewrite_custom_function_call(nn, args),
        }
    }

    /// Rewrites a custom SPARQL function call
    #[allow(clippy::unused_self)]
    fn rewrite_custom_function_call(
        &self,
        function: &NamedNode,
        args: Vec<Expr>,
    ) -> DFResult<Expr> {
        if function == &xsd::BOOLEAN {
            let arg = unary_args(args)?;
            return self.expr_builder.as_boolean(arg);
        }

        if function == &xsd::INT {
            let arg = unary_args(args)?;
            return self.expr_builder.as_int(arg);
        }

        if function == &xsd::INTEGER {
            let arg = unary_args(args)?;
            return self.expr_builder.as_integer(arg);
        }

        if function == &xsd::FLOAT {
            let arg = unary_args(args)?;
            return self.expr_builder.as_float(arg);
        }

        if function == &xsd::DOUBLE {
            let arg = unary_args(args)?;
            return self.expr_builder.as_double(arg);
        }

        if function == &xsd::DECIMAL {
            let arg = unary_args(args)?;
            return self.expr_builder.as_decimal(arg);
        }

        if function == &xsd::DATE_TIME {
            let arg = unary_args(args)?;
            return self.expr_builder.as_date_time(arg);
        }

        if function == &xsd::STRING {
            let arg = unary_args(args)?;
            return self.expr_builder.as_string(arg);
        }

        plan_err!("Custom Function {} is not supported.", function.as_str())
    }

    /// Rewrites an IN expression to a list of equality checks. As the IN operation is equal to
    /// checking equality (using the "=" operator) this rewrite is sound.
    ///
    /// We cannot use the default DataFusion [Expr::InList] (without additional canonicalization) as
    /// the `=` is used.
    ///
    /// https://www.w3.org/TR/sparql11-query/#func-in
    fn rewrite_in(&self, lhs: &Expression, rhs: &[Expression]) -> DFResult<Expr> {
        let lhs = self.rewrite(lhs)?;
        let expressions = rhs
            .iter()
            .map(|e| self.expr_builder.equal(lhs.clone(), self.rewrite(e)?))
            .map(|res| res.and_then(|e| self.expr_builder.native_boolean_as_term(e)))
            .collect::<DFResult<Vec<_>>>()?;

        let false_literal = Literal::from(false);
        expressions
            .into_iter()
            .reduce(or)
            .map_or(self.expr_builder.literal(false_literal.as_ref()), |expr| {
                self.expr_builder.native_boolean_as_term(expr)
            })
    }

    /// Rewrites an EXISTS expression to a correlated subquery.
    fn rewrite_exists(&self, inner: &GraphPattern) -> DFResult<Expr> {
        let inner = LogicalPlanBuilder::new(self.graph_rewriter.rewrite(inner)?);

        let outer_keys: HashSet<_> = self
            .expr_builder
            .schema()
            .columns()
            .into_iter()
            .map(|c| c.name().to_owned())
            .collect();
        let inner_keys: HashSet<_> = inner
            .schema()
            .columns()
            .into_iter()
            .map(|c| c.name().to_owned())
            .collect();

        // TODO: Is there a better way to check for this?
        if outer_keys.is_disjoint(&inner_keys) {
            let group_expr: [Expr; 0] = [];
            let count =
                inner.aggregate(group_expr, [count(Expr::Literal(COUNT_STAR_EXPANSION))])?;
            let subquery = Subquery {
                subquery: count.build()?.into(),
                outer_ref_columns: vec![],
                spans: Spans(vec![]),
            };
            return self
                .expr_builder
                .native_boolean_as_term(Expr::ScalarSubquery(subquery).gt(lit(0)));
        }

        // TODO: Investigate why we need this renaming and cannot refer to the unqualified column
        let projections = inner
            .schema()
            .columns()
            .into_iter()
            .map(|c| Expr::from(c.clone()).alias(format!("__inner__{}", c.name())))
            .collect::<Vec<_>>();
        let projected_inner = inner.project(projections)?;

        let compatible_filters = outer_keys
            .intersection(&inner_keys)
            .map(|k| {
                let data_type = self
                    .expr_builder
                    .schema()
                    .field_with_name(None, k)
                    .map_err(|_| plan_datafusion_err!("Could not find column {} in schema.", k))?
                    .data_type();
                self.expr_builder.is_compatible(
                    Expr::OuterReferenceColumn(data_type.clone(), Column::new_unqualified(k)),
                    Expr::from(Column::new_unqualified(format!("__inner__{k}"))),
                )
            })
            .collect::<DFResult<Vec<_>>>()?;
        let compatible_filter = compatible_filters
            .into_iter()
            .reduce(and)
            .unwrap_or(lit(true));

        let subquery = Arc::new(projected_inner.filter(compatible_filter)?.build()?);
        self.expr_builder.native_boolean_as_term(exists(subquery))
    }

    /// Rewrites an IF expression to a case expression.
    fn rewrite_if(
        &self,
        test: &Expression,
        if_true: &Expression,
        if_false: &Expression,
    ) -> DFResult<Expr> {
        let test = self.rewrite(test)?;
        let if_true = self.rewrite(if_true)?;
        let if_false = self.rewrite(if_false)?;
        self.expr_builder.sparql_if(test, if_true, if_false)
    }
}

/// TODO
fn unary_args(args: Vec<Expr>) -> DFResult<Expr> {
    match TryInto::<[Expr; 1]>::try_into(args) {
        Ok([expr]) => Ok(expr),
        Err(_) => plan_err!("Unsupported argument list for unary function."),
    }
}

/// TODO
fn binary_args(args: Vec<Expr>) -> DFResult<(Expr, Expr)> {
    match TryInto::<[Expr; 2]>::try_into(args) {
        Ok([lhs, rhs]) => Ok((lhs, rhs)),
        Err(_) => plan_err!("Unsupported argument list for unary function."),
    }
}

/// TODO
fn ternary_args(args: Vec<Expr>) -> DFResult<(Expr, Expr, Expr)> {
    match TryInto::<[Expr; 3]>::try_into(args) {
        Ok([arg0, arg1, arg2]) => Ok((arg0, arg1, arg2)),
        Err(_) => plan_err!("Unsupported argument list for unary function."),
    }
}

/// TODO
fn quarternary_args(args: Vec<Expr>) -> DFResult<(Expr, Expr, Expr, Expr)> {
    match TryInto::<[Expr; 4]>::try_into(args) {
        Ok([arg0, arg1, arg2, arg3]) => Ok((arg0, arg1, arg2, arg3)),
        Err(_) => plan_err!("Unsupported argument list for unary function."),
    }
}

fn logical_expression(
    rewriter: &ExpressionRewriter<'_>,
    operator: Operator,
    lhs: &Expression,
    rhs: &Expression,
) -> DFResult<Expr> {
    let lhs = rewriter
        .expr_builder
        .effective_boolean_value(rewriter.rewrite(lhs)?)?;
    let rhs = rewriter
        .expr_builder
        .effective_boolean_value(rewriter.rewrite(rhs)?)?;

    let result = match operator {
        Operator::And => rewriter.expr_builder.and(lhs, rhs)?,
        Operator::Or => rewriter.expr_builder.or(lhs, rhs)?,
        _ => return plan_err!("Unsupported logical expression: {}", &operator),
    };
    rewriter.expr_builder.native_boolean_as_term(result)
}
