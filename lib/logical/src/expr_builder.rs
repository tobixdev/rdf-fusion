use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_err, Column, DFSchema};
use datafusion::functions_aggregate::count::{count, count_distinct};
use datafusion::functions_window::expr_fn::first_value;
use datafusion::logical_expr::{and, lit, or, Expr, ExprSchemable, ScalarUDF};
use graphfusion_encoding::plain_term::encoders::DefaultPlainTermEncoder;
use graphfusion_encoding::plain_term::PlainTermEncoding;
use graphfusion_encoding::typed_value::TypedValueEncoding;
use graphfusion_encoding::{EncodingName, EncodingScalar, TermEncoder, TermEncoding};
use graphfusion_functions::builtin::BuiltinName;
use graphfusion_functions::registry::GraphFusionBuiltinRegistry;
use graphfusion_model::{Iri, TermRef, ThinError, Variable};
use std::collections::HashMap;
use std::ops::Not;
use std::sync::Arc;
// TODO maybe this expr stuff is good in a separate crate

// TODO this is still a bit messy with when a boolean and when a term is returned. Fix that.

/// TODO: Explain why
#[derive(Debug, Clone)]
pub struct GraphFusionExprBuilder<'a> {
    /// The schema of the input data. Necessary for inferring the encodings of RDF terms.
    schema: &'a DFSchema,
    /// Provides access to the builtin functions.
    registry: &'a GraphFusionBuiltinRegistry,
}

impl<'a> GraphFusionExprBuilder<'a> {
    /// TODO
    pub fn count(&self, expr: Expr, distinct: bool) -> DFResult<Expr> {
        Ok(match distinct {
            true => count_distinct(expr),
            false => count(expr),
        })
    }

    /// TODO
    pub fn avg(&self, expr: Expr, distinct: bool) -> DFResult<Expr> {
        Expr::AggregateFunction(datafusion::logical_expr::expr::AggregateFunction::new_udf(
            Arc::new(ENC_AVG.deref().clone()),
            vec![expr],
            distinct,
            None,
            None,
            None,
        ))
    }

    /// TODO
    pub fn max(&self, expr: Expr) -> DFResult<Expr> {
        Expr::AggregateFunction(datafusion::logical_expr::expr::AggregateFunction::new_udf(
            Arc::new(ENC_MAX.deref().clone()),
            vec![expr],
            false,
            None,
            None,
            None,
        ))
    }

    /// TODO
    pub fn min(&self, expr: Expr) -> DFResult<Expr> {
        Expr::AggregateFunction(datafusion::logical_expr::expr::AggregateFunction::new_udf(
            Arc::new(ENC_MIN.deref().clone()),
            vec![expr],
            false,
            None,
            None,
            None,
        ))
    }

    /// TODO
    pub fn sample(&self, expr: Expr) -> DFResult<Expr> {
        Ok(first_value(expr))
    }

    /// TODO
    pub fn sum(&self, expr: Expr, distinct: bool) -> DFResult<Expr> {
        Expr::AggregateFunction(datafusion::logical_expr::expr::AggregateFunction::new_udf(
            Arc::new(ENC_MIN.deref().clone()),
            vec![expr],
            distinct,
            None,
            None,
            None,
        ))
    }

    pub fn group_concat(&self, p0: Expr, p1: bool, p2: Option<&str>) {
        todo!()
    }
}

impl<'a> GraphFusionExprBuilder<'a> {
    pub fn new(schema: &'a DFSchema, registry: &'a GraphFusionBuiltinRegistry) -> Self {
        Self { schema, registry }
    }

    /// Returns the schema of the input data.
    pub fn schema(&self) -> &DFSchema {
        self.schema
    }

    pub fn with_encoding(&self, value: Expr, encoding: EncodingName) -> DFResult<Expr> {
        todo!()
    }

    pub fn sparql_if(&self, p0: Vec<Expr>) -> Expr {
        todo!()
    }

    pub fn is_compatible(&self, p0: Expr, p1: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn coalesce(&self, args: Vec<Expr>) -> Expr {
        todo!()
    }

    pub fn as_string(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn as_date_time(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn as_decimal(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn as_double(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn as_float(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn as_integer(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn as_int(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn as_boolean(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn sha512(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn sha384(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn sha256(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn sha1(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn md5(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn tz(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn timezone(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn seconds(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn minutes(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn hours(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn day(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn month(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn year(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn rand(&self) -> DFResult<Expr> {
        todo!()
    }

    pub fn replace_with_flags(&self, p0: Expr, p1: Expr, p2: Expr, p3: Expr) -> Expr {
        todo!()
    }

    /// TODO
    pub fn replace(&self, p0: Expr, p1: Expr, p2: Expr) -> Expr {
        todo!()
    }

    /// TODO
    pub fn abs(&self, val: Expr) -> DFResult<Expr> {
        todo!()
    }

    /// TODO
    pub fn round(&self, val: Expr) -> DFResult<Expr> {
        todo!()
    }

    /// TODO
    pub fn ceil(&self, val: Expr) -> DFResult<Expr> {
        todo!()
    }

    /// TODO
    pub fn floor(&self, val: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn regex_with_flags(&self, p0: Expr, p1: Expr, p2: Expr) -> Expr {
        todo!()
    }

    pub fn regex(&self, p0: Expr, p1: Expr) -> Expr {
        todo!()
    }

    pub fn lang_matches(&self, p0: Expr, p1: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn encode_for_uri(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn concat(&self, args: Vec<Expr>) -> DFResult<Expr> {
        todo!()
    }

    pub fn str_after(&self, p0: Expr, p1: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn str_before(&self, p0: Expr, p1: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn str_ends(&self, p0: Expr, p1: Expr) -> DFResult<Expr> {
        todo!()
    }
    pub fn contains(&self, p0: Expr, p1: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn str_starts(&self, p0: Expr, p1: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn lcase(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn ucase(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn substr_with_length(&self, p0: Expr, p1: Expr, p2: Expr) -> Expr {
        todo!()
    }

    pub fn substr(&self, p0: Expr, p1: Expr) -> Expr {
        todo!()
    }

    pub fn str_len(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn str_uuid(&self) -> DFResult<Expr> {
        todo!()
    }

    pub fn uuid(&self) -> DFResult<Expr> {
        todo!()
    }

    pub fn str_lang(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn str_dt(&self, p0: Expr, p1: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn bnode_from(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn bnode(&self) -> DFResult<Expr> {
        todo!()
    }

    pub fn iri(&self, p0: Option<&Iri<String>>, p1: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn datatype(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn lang(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn str(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn is_numeric(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn is_literal(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    pub fn is_blank(&self, p0: Expr) -> DFResult<Expr> {
        todo!()
    }

    /// TODO
    pub fn is_iri(&self, value: Expr) -> DFResult<Expr> {
        self.unary_udf(BuiltinName::IsIri, value)
    }

    /// TODO
    pub fn unary_plus(&self, value: Expr) -> DFResult<Expr> {
        self.unary_udf(BuiltinName::UnaryPlus, value)
    }

    /// TODO
    pub fn unary_minus(&self, value: Expr) -> DFResult<Expr> {
        self.unary_udf(BuiltinName::UnaryMinus, value)
    }

    /// TODO
    pub fn add(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        self.binary_udf(BuiltinName::Add, lhs, rhs)
    }

    /// TODO
    pub fn sub(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        self.binary_udf(BuiltinName::Sub, lhs, rhs)
    }

    /// TODO
    pub fn mul(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        self.binary_udf(BuiltinName::Mul, lhs, rhs)
    }

    /// TODO
    pub fn div(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        self.binary_udf(BuiltinName::Div, lhs, rhs)
    }

    /// TODO
    pub fn and(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let lhs = self.ensure_boolean(lhs)?;
        let rhs = self.ensure_boolean(rhs)?;
        Ok(and(lhs, rhs))
    }

    /// TODO
    pub fn or(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let lhs = self.ensure_boolean(lhs)?;
        let rhs = self.ensure_boolean(rhs)?;
        Ok(or(lhs, rhs))
    }

    /// TODO
    fn ensure_boolean(&self, expr: Expr) -> DFResult<Expr> {
        let (data_type, _) = expr.data_type_and_nullable(self.schema)?;
        if data_type == DataType::Boolean {
            return Ok(expr);
        }

        todo!()
    }

    /// TODO
    pub fn variable(&self, var: &Variable) -> DFResult<Expr> {
        let column = Column::new_unqualified(var.as_str());
        if self.schema().has_column(&column) {
            Ok(Expr::from(column))
        } else {
            let null = DefaultPlainTermEncoder::encode_term(ThinError::expected())?;
            Ok(lit(null.into_scalar_value()))
        }
    }

    /// TODO
    pub fn literal<'lit>(&self, term: impl Into<TermRef<'lit>>) -> DFResult<Expr> {
        let scalar = DefaultPlainTermEncoder::encode_term(Ok(term.into()))?;
        Ok(lit(scalar.into_scalar_value()))
    }

    /// TODO
    pub fn equal(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        self.binary_udf(BuiltinName::Equal, lhs, rhs)
    }

    /// TODO
    pub fn greater_than(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        self.binary_udf(BuiltinName::GreaterThan, lhs, rhs)
    }

    /// TODO
    pub fn greater_or_equal(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        self.binary_udf(BuiltinName::GreaterOrEqual, lhs, rhs)
    }

    /// TODO
    pub fn less_than(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        self.binary_udf(BuiltinName::LessThan, lhs, rhs)
    }

    /// TODO
    pub fn less_or_equal(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        self.binary_udf(BuiltinName::LessOrEqual, lhs, rhs)
    }

    /// TODO
    pub fn bound(&self, var: &Variable) -> DFResult<Expr> {
        let var = self.variable(var)?;
        self.unary_udf(BuiltinName::Bound, var)
    }

    /// TODO
    pub fn not(&self, inner: Expr) -> DFResult<Expr> {
        let ebv = self.effective_boolean_value(inner)?;
        let not = Expr::not(ebv);
        self.native_boolean_as_term(not)
    }

    /// TODO
    pub fn native_boolean_as_term(&self, expr: Expr) -> DFResult<Expr> {
        let (data_type, _) = expr.data_type_and_nullable(self.schema)?;
        if data_type != DataType::Boolean {
            return plan_err!(
                "Expression must be Boolean for {}.",
                BuiltinName::NativeBooleanAsTerm
            );
        }

        let udf = self.create_scalar_udf(BuiltinName::NativeBooleanAsTerm)?;
        Ok(udf.call(vec![expr]))
    }

    /// TODO
    pub fn effective_boolean_value(&self, expr: Expr) -> DFResult<Expr> {
        self.unary_udf(BuiltinName::EffectiveBooleanValue, expr)
    }

    /// TODO
    pub fn same_term(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let effective_boolean_value = self.create_scalar_udf(BuiltinName::EffectiveBooleanValue)?;
        let same_term = self.create_scalar_udf(BuiltinName::SameTerm)?;
        Ok(effective_boolean_value.call(vec![same_term.call(vec![lhs, rhs])]))
    }

    /// TODO
    pub fn filter_by_scalar(&self, expr: Expr, scalar: TermRef<'_>) -> DFResult<Expr> {
        let encoding_name = self.encoding(&expr)?;
        let literal = match encoding_name {
            EncodingName::PlainTerm => {
                PlainTermEncoding::encode_scalar(scalar)?.into_scalar_value()
            }
            EncodingName::TypedValue => {
                TypedValueEncoding::encode_scalar(scalar)?.into_scalar_value()
            }
            EncodingName::Sortable => {
                return plan_err!("Filtering not supported for Sortable encoding.")
            }
        };
        self.same_term(expr, lit(literal))
    }

    /// Tries to obtain the encoding from a given expression.
    fn encoding(&self, expr: &Expr) -> DFResult<EncodingName> {
        let (data_type, _) = expr.data_type_and_nullable(self.schema)?;

        if data_type == PlainTermEncoding::data_type() {
            return Ok(PlainTermEncoding::name());
        }

        if data_type == TypedValueEncoding::data_type() {
            return Ok(TypedValueEncoding::name());
        }

        plan_err!(
            "Expression does not have a valid RDF term encoding: {}",
            &data_type
        )
    }

    /// TODO
    fn unary_udf(&self, name: BuiltinName, value: Expr) -> DFResult<Expr> {
        let udf = self.create_scalar_udf(name)?;
        Ok(udf.call(vec![value]))
    }

    /// TODO
    fn binary_udf(&self, name: BuiltinName, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let udf = self.create_scalar_udf(name)?;
        Ok(udf.call(vec![lhs, rhs]))
    }

    /// TODO
    fn create_scalar_udf(&self, name: BuiltinName) -> DFResult<ScalarUDF> {
        self.registry
            .scalar_factory(name)
            .create_with_args(HashMap::new())
    }
}
