use crate::sparql::rewriting::GraphPatternRewriter;
use crate::DFResult;
use arrow_rdf::encoded::scalars::{
    encode_scalar_literal, encode_scalar_named_node, encode_scalar_null,
};
use arrow_rdf::encoded::{
    enc_iri, EncTerm, ENC_ABS, ENC_ADD, ENC_AND, ENC_AS_BOOLEAN, ENC_AS_DATETIME, ENC_AS_DECIMAL,
    ENC_AS_DOUBLE, ENC_AS_FLOAT, ENC_AS_INT, ENC_AS_INTEGER, ENC_AS_STRING, ENC_BNODE_NULLARY,
    ENC_BNODE_UNARY, ENC_BOOLEAN_AS_RDF_TERM, ENC_BOUND, ENC_CEIL, ENC_COALESCE, ENC_CONCAT,
    ENC_CONTAINS, ENC_DATATYPE, ENC_DAY, ENC_DIV, ENC_EFFECTIVE_BOOLEAN_VALUE, ENC_ENCODEFORURI,
    ENC_EQ, ENC_FLOOR, ENC_GREATER_OR_EQUAL, ENC_GREATER_THAN, ENC_HOURS, ENC_IS_BLANK,
    ENC_IS_COMPATIBLE, ENC_IS_IRI, ENC_IS_LITERAL, ENC_IS_NUMERIC, ENC_LANG, ENC_LANGMATCHES,
    ENC_LCASE, ENC_LESS_OR_EQUAL, ENC_LESS_THAN, ENC_MD5, ENC_MINUTES, ENC_MONTH, ENC_MUL, ENC_OR,
    ENC_RAND, ENC_REGEX_BINARY, ENC_REGEX_TERNARY, ENC_REPLACE_QUATERNARY, ENC_REPLACE_TERNARY,
    ENC_ROUND, ENC_SAME_TERM, ENC_SECONDS, ENC_SHA1, ENC_SHA256, ENC_SHA384, ENC_SHA512, ENC_STR,
    ENC_STRAFTER, ENC_STRBEFORE, ENC_STRDT, ENC_STRENDS, ENC_STRLANG, ENC_STRLEN, ENC_STRSTARTS,
    ENC_STRUUID, ENC_SUB, ENC_SUBSTR_BINARY, ENC_SUBSTR_TERNARY, ENC_TIMEZONE, ENC_TZ, ENC_UCASE,
    ENC_UNARY_MINUS, ENC_UNARY_PLUS, ENC_UUID, ENC_YEAR,
};
use datafusion::common::{internal_err, not_impl_err, plan_err, Column, DFSchema};
use datafusion::logical_expr::{case, lit, or, Expr, LogicalPlanBuilder, Operator, ScalarUDF};
use datafusion::prelude::{and, exists};
use datamodel::DateTime;
use oxiri::Iri;
use oxrdf::vocab::xsd;
use oxrdf::{Literal, NamedNode};
use spargebra::algebra::{Expression, Function, GraphPattern};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub(super) struct ExpressionRewriter<'rewriter> {
    graph_rewriter: &'rewriter GraphPatternRewriter,
    base_iri: Option<&'rewriter Iri<String>>,
    schema: &'rewriter DFSchema,
}

impl<'rewriter> ExpressionRewriter<'rewriter> {
    /// Creates a new expression rewriter for a given schema.
    pub fn new(
        graph_rewriter: &'rewriter GraphPatternRewriter,
        base_iri: Option<&'rewriter Iri<String>>,
        schema: &'rewriter DFSchema,
    ) -> Self {
        Self {
            graph_rewriter,
            base_iri,
            schema,
        }
    }

    /// Rewrites an [Expression].
    pub fn rewrite(&self, expression: &Expression) -> DFResult<Expr> {
        match expression {
            Expression::Bound(var) => {
                Ok(ENC_BOUND.call(vec![Expr::from(Column::new_unqualified(var.as_str()))]))
            }
            Expression::Not(inner) => Ok(ENC_BOOLEAN_AS_RDF_TERM.call(vec![Expr::Not(Box::new(
                ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![self.rewrite(inner)?]),
            ))])),
            Expression::Equal(lhs, rhs) => binary_udf(self, &ENC_EQ, lhs, rhs),
            Expression::SameTerm(lhs, rhs) => binary_udf(self, &ENC_SAME_TERM, lhs, rhs),
            Expression::Greater(lhs, rhs) => binary_udf(self, &ENC_GREATER_THAN, lhs, rhs),
            Expression::GreaterOrEqual(lhs, rhs) => {
                binary_udf(self, &ENC_GREATER_OR_EQUAL, lhs, rhs)
            }
            Expression::Less(lhs, rhs) => binary_udf(self, &ENC_LESS_THAN, lhs, rhs),
            Expression::LessOrEqual(lhs, rhs) => binary_udf(self, &ENC_LESS_OR_EQUAL, lhs, rhs),
            Expression::Literal(literal) => Ok(lit(encode_scalar_literal(literal.as_ref())?)),
            Expression::Variable(var) => {
                let column = Column::new_unqualified(var.as_str());
                if self.schema.has_column(&column) {
                    Ok(Expr::from(column))
                } else {
                    Ok(lit(encode_scalar_null()))
                }
            }
            Expression::FunctionCall(function, args) => self.rewrite_function_call(function, args),
            Expression::NamedNode(nn) => Ok(lit(encode_scalar_named_node(nn.as_ref()))),
            Expression::Or(lhs, rhs) => logical_expression(self, Operator::Or, lhs, rhs),
            Expression::And(lhs, rhs) => logical_expression(self, Operator::And, lhs, rhs),
            Expression::In(lhs, rhs) => self.rewrite_in(lhs, rhs),
            Expression::Add(lhs, rhs) => binary_udf(self, &ENC_ADD, lhs, rhs),
            Expression::Subtract(lhs, rhs) => binary_udf(self, &ENC_SUB, lhs, rhs),
            Expression::Multiply(lhs, rhs) => binary_udf(self, &ENC_MUL, lhs, rhs),
            Expression::Divide(lhs, rhs) => binary_udf(self, &ENC_DIV, lhs, rhs),
            Expression::UnaryPlus(value) => unary_udf(self, &ENC_UNARY_PLUS, value),
            Expression::UnaryMinus(value) => unary_udf(self, &ENC_UNARY_MINUS, value),
            Expression::Exists(pattern) => self.rewrite_exists(pattern),
            Expression::If(test, if_true, if_false) => self.rewrite_if(test, if_true, if_false),
            Expression::Coalesce(args) => {
                let args = args
                    .iter()
                    .map(|arg| self.rewrite(arg))
                    .collect::<DFResult<Vec<_>>>()?;
                Ok(ENC_COALESCE.call(args))
            }
        }
    }

    /// Rewrites a SPARQL function call.
    ///
    /// We assume here that the length of `args` matches the expected number of arguments.
    fn rewrite_function_call(&self, function: &Function, args: &Vec<Expression>) -> DFResult<Expr> {
        let args = args
            .iter()
            .map(|e| self.rewrite(e))
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
            Function::Iri => Ok(enc_iri(self.base_iri.cloned()).call(args)),
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
            Function::SubStr => Ok(match args.len() {
                2 => ENC_SUBSTR_BINARY.call(args),
                3 => ENC_SUBSTR_TERNARY.call(args),
                _ => unreachable!("Unexpected number of args"),
            }),
            Function::UCase => Ok(ENC_UCASE.call(args)),
            Function::LCase => Ok(ENC_LCASE.call(args)),
            Function::StrStarts => Ok(ENC_STRSTARTS.call(args)),
            Function::StrEnds => Ok(ENC_STRENDS.call(args)),
            Function::Contains => Ok(ENC_CONTAINS.call(args)),
            Function::StrBefore => Ok(ENC_STRBEFORE.call(args)),
            Function::StrAfter => Ok(ENC_STRAFTER.call(args)),
            Function::EncodeForUri => Ok(ENC_ENCODEFORURI.call(args)),
            Function::Concat => Ok(ENC_CONCAT.call(args)),
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
            // Dates & Durations
            Function::Year => Ok(ENC_YEAR.call(args)),
            Function::Month => Ok(ENC_MONTH.call(args)),
            Function::Day => Ok(ENC_DAY.call(args)),
            Function::Hours => Ok(ENC_HOURS.call(args)),
            Function::Minutes => Ok(ENC_MINUTES.call(args)),
            Function::Seconds => Ok(ENC_SECONDS.call(args)),
            Function::Timezone => Ok(ENC_TIMEZONE.call(args)),
            Function::Tz => Ok(ENC_TZ.call(args)),
            Function::Now => {
                let literal =
                    Literal::new_typed_literal(DateTime::now().to_string(), xsd::DATE_TIME);
                Ok(lit(encode_scalar_literal(literal.as_ref())?))
            }
            // Hashing
            Function::Md5 => Ok(ENC_MD5.call(args)),
            Function::Sha1 => Ok(ENC_SHA1.call(args)),
            Function::Sha256 => Ok(ENC_SHA256.call(args)),
            Function::Sha384 => Ok(ENC_SHA384.call(args)),
            Function::Sha512 => Ok(ENC_SHA512.call(args)),
            // Custom
            Function::Custom(nn) => self.rewrite_custom_function_call(nn, args),
            _ => not_impl_err!("rewrite_function_call: {:?}", function),
        }
    }

    /// Rewrites a custom SPARQL function call
    fn rewrite_custom_function_call(
        &self,
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

    /// Rewrites an IN expression to a list of equality checks. As the IN operation is equal to
    /// checking equality (using the "=" operator) this rewrite is sound.
    ///
    /// We cannot use the default DataFusion [Expr::InList] (without additional canonicalization) as
    /// the `=` is used.
    ///
    /// https://www.w3.org/TR/sparql11-query/#func-in
    fn rewrite_in(&self, lhs: &Expression, rhs: &Vec<Expression>) -> DFResult<Expr> {
        let lhs = self.rewrite(lhs)?;
        let expressions = rhs
            .iter()
            .map(|e| Ok(ENC_EQ.call(vec![lhs.clone(), self.rewrite(e)?])))
            .collect::<DFResult<Vec<_>>>()?;

        let false_literal = Literal::from(false);
        let result = expressions
            .into_iter()
            .reduce(|lhs, rhs| or(lhs, rhs))
            .unwrap_or(lit(encode_scalar_literal(false_literal.as_ref())?));

        Ok(result)
    }

    /// Rewrites an EXISTS expression to a correlated subquery.
    fn rewrite_exists(&self, inner: &GraphPattern) -> DFResult<Expr> {
        let inner = LogicalPlanBuilder::new(self.graph_rewriter.rewrite(inner)?);

        let outer_keys: HashSet<_> = self
            .schema
            .columns()
            .into_iter()
            .map(|c| c.name().to_string())
            .collect();
        let inner_keys: HashSet<_> = inner
            .schema()
            .columns()
            .into_iter()
            .map(|c| c.name().to_string())
            .collect();

        // TODO: Investigate why we need this renaming and cannot refer to the unqualified column
        let projections = inner
            .schema()
            .columns()
            .into_iter()
            .map(|c| Expr::from(c.clone()).alias(format!("__inner__{}", c.name())))
            .collect::<Vec<_>>();
        let projected_inner = inner.project(projections)?;

        let compatible_filter = outer_keys
            .intersection(&inner_keys)
            .map(|k| {
                ENC_IS_COMPATIBLE.call(vec![
                    Expr::OuterReferenceColumn(EncTerm::term_type(), Column::new_unqualified(k)),
                    Expr::from(Column::new_unqualified(format!("__inner__{}", k))),
                ])
            })
            .reduce(|lhs, rhs| and(lhs, rhs))
            .unwrap_or(lit(true));

        let subquery = Arc::new(projected_inner.filter(compatible_filter)?.build()?);
        Ok(ENC_BOOLEAN_AS_RDF_TERM.call(vec![exists(subquery)]))
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

        case(ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![test]))
            .when(lit(true), if_true)
            .when(lit(false), if_false)
            .otherwise(lit(encode_scalar_null()))
    }
}

fn logical_expression(
    rewriter: &ExpressionRewriter<'_>,
    operator: Operator,
    lhs: &Expression,
    rhs: &Expression,
) -> DFResult<Expr> {
    let lhs = ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![rewriter.rewrite(lhs)?]);
    let rhs = ENC_EFFECTIVE_BOOLEAN_VALUE.call(vec![rewriter.rewrite(rhs)?]);

    let connective_impl = match operator {
        Operator::And => ENC_AND,
        Operator::Or => ENC_OR,
        _ => plan_err!("Unsupported logical expression: {}", &operator)?,
    };
    let booleans = connective_impl.call(vec![lhs, rhs]);
    Ok(ENC_BOOLEAN_AS_RDF_TERM.call(vec![booleans]))
}

fn unary_udf(
    rewriter: &ExpressionRewriter<'_>,
    udf: &ScalarUDF,
    value: &Box<Expression>,
) -> DFResult<Expr> {
    let value = rewriter.rewrite(value)?;
    Ok(udf.call(vec![value]))
}

fn binary_udf(
    rewriter: &ExpressionRewriter<'_>,
    udf: &ScalarUDF,
    lhs: &Expression,
    rhs: &Expression,
) -> DFResult<Expr> {
    let lhs = rewriter.rewrite(lhs)?;
    let rhs = rewriter.rewrite(rhs)?;
    Ok(udf.call(vec![lhs, rhs]))
}
