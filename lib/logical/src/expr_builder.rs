use crate::{ActiveGraph, DFResult};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_datafusion_err, plan_err, Column, DFSchema};
use datafusion::functions_aggregate::count::{count, count_distinct};
use datafusion::functions_window::expr_fn::first_value;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{lit, Expr, ExprSchemable};
use graphfusion_encoding::plain_term::encoders::DefaultPlainTermEncoder;
use graphfusion_encoding::plain_term::PlainTermEncoding;
use graphfusion_encoding::typed_value::TypedValueEncoding;
use graphfusion_encoding::{EncodingName, EncodingScalar, TermEncoder, TermEncoding};
use graphfusion_functions::builtin::BuiltinName;
use graphfusion_functions::registry::GraphFusionFunctionRegistry;
use graphfusion_functions::FunctionName;
use graphfusion_model::{Iri, Literal, Term, TermRef, ThinError, VariableRef};
use spargebra::term::NamedNode;
use std::collections::HashMap;
use std::ops::Not;
// TODO maybe this expr stuff is good in a separate crate

// TODO this is still a bit messy with when a boolean and when a term is returned. Fix that.

/// TODO: Explain why
#[derive(Debug, Clone, Copy)]
pub struct GraphFusionExprBuilder<'a> {
    /// The schema of the input data. Necessary for inferring the encodings of RDF terms.
    schema: &'a DFSchema,
    /// Provides access to the builtin functions.
    registry: &'a GraphFusionFunctionRegistry,
}

impl GraphFusionExprBuilder<'_> {
    /// TODO
    pub fn count(&self, expr: Expr, distinct: bool) -> DFResult<Expr> {
        Ok(if distinct {
            count_distinct(expr)
        } else {
            count(expr)
        })
    }

    /// TODO
    pub(crate) fn filter_active_graph(
        &self,
        expr: Expr,
        active_graph: &ActiveGraph,
    ) -> DFResult<Expr> {
        match active_graph {
            ActiveGraph::DefaultGraph => Ok(expr.is_null()),
            ActiveGraph::NamedGraphs(named_graphs) => {
                let filters = named_graphs
                    .iter()
                    .map(|g| self.filter_by_scalar(expr.clone(), g.as_ref().into()))
                    .collect::<DFResult<Vec<_>>>()?;
                Ok(filters
                    .into_iter()
                    .reduce(|a, b| self.and(a, b).expect("TODO"))
                    .expect("At least one active graph"))
            }
            ActiveGraph::AnyNamedGraph => Ok(expr.is_not_null()),
        }
    }

    /// TODO
    pub fn avg(&self, expr: Expr, distinct: bool) -> DFResult<Expr> {
        self.apply_builtin_udaf(BuiltinName::Avg, expr, distinct, HashMap::new())
    }

    /// TODO
    pub fn max(&self, expr: Expr) -> DFResult<Expr> {
        self.apply_builtin_udaf(BuiltinName::Max, expr, false, HashMap::new())
    }

    /// TODO
    pub fn min(&self, expr: Expr) -> DFResult<Expr> {
        self.apply_builtin_udaf(BuiltinName::Min, expr, false, HashMap::new())
    }

    /// TODO
    pub fn sample(&self, expr: Expr) -> DFResult<Expr> {
        Ok(first_value(expr))
    }

    /// TODO
    pub fn sum(&self, expr: Expr, distinct: bool) -> DFResult<Expr> {
        self.apply_builtin_udaf(BuiltinName::Sum, expr, distinct, HashMap::new())
    }

    pub fn group_concat(
        &self,
        expr: Expr,
        distinct: bool,
        separator: Option<&str>,
    ) -> DFResult<Expr> {
        let separator = separator.map(|s| Term::from(Literal::new_simple_literal(s)));
        let mut args = HashMap::new();
        if let Some(separator) = separator {
            args.insert("separator".to_owned(), separator);
        }

        self.apply_builtin_udaf(BuiltinName::Sum, expr, distinct, args)
    }
}

impl<'a> GraphFusionExprBuilder<'a> {
    pub fn new(schema: &'a DFSchema, registry: &'a GraphFusionFunctionRegistry) -> Self {
        Self { schema, registry }
    }

    /// Returns the schema of the input data.
    pub fn schema(&self) -> &DFSchema {
        self.schema
    }

    pub fn sparql_if(&self, test: Expr, if_true: Expr, if_false: Expr) -> DFResult<Expr> {
        self.apply_builtin(BuiltinName::Coalesce, vec![test, if_true, if_false])
    }

    pub fn coalesce(&self, args: Vec<Expr>) -> DFResult<Expr> {
        self.apply_builtin(BuiltinName::Coalesce, args)
    }

    pub fn as_string(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::AsString;
        self.apply_builtin(name, vec![p0])
    }

    pub fn as_date_time(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::AsDateTime;
        self.apply_builtin(name, vec![p0])
    }

    pub fn as_decimal(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::AsDecimal;
        self.apply_builtin(name, vec![p0])
    }

    pub fn as_double(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::AsDouble;
        self.apply_builtin(name, vec![p0])
    }

    pub fn as_float(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::AsFloat;
        self.apply_builtin(name, vec![p0])
    }

    pub fn as_integer(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::AsInteger;
        self.apply_builtin(name, vec![p0])
    }

    pub fn as_int(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::AsInt;
        self.apply_builtin(name, vec![p0])
    }

    pub fn as_boolean(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::AsBoolean;
        self.apply_builtin(name, vec![p0])
    }

    pub fn sha512(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Sha512;
        self.apply_builtin(name, vec![p0])
    }

    pub fn sha384(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Sha384;
        self.apply_builtin(name, vec![p0])
    }

    pub fn sha256(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Sha256;
        self.apply_builtin(name, vec![p0])
    }

    pub fn sha1(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Sha1;
        self.apply_builtin(name, vec![p0])
    }

    pub fn md5(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Md5;
        self.apply_builtin(name, vec![p0])
    }

    pub fn tz(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Tz;
        self.apply_builtin(name, vec![p0])
    }

    pub fn timezone(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Timezone;
        self.apply_builtin(name, vec![p0])
    }

    pub fn seconds(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Seconds;
        self.apply_builtin(name, vec![p0])
    }

    pub fn minutes(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Minutes;
        self.apply_builtin(name, vec![p0])
    }

    pub fn hours(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Hours;
        self.apply_builtin(name, vec![p0])
    }

    pub fn day(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Day;
        self.apply_builtin(name, vec![p0])
    }

    pub fn month(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Month;
        self.apply_builtin(name, vec![p0])
    }

    pub fn year(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Year;
        self.apply_builtin(name, vec![p0])
    }

    pub fn rand(&self) -> DFResult<Expr> {
        let udf = self
            .registry
            .udf_factory(FunctionName::Builtin(BuiltinName::Rand))
            .create_with_args(HashMap::new())?;
        Ok(udf.call(vec![]))
    }

    pub fn replace_with_flags(&self, p0: Expr, p1: Expr, p2: Expr, p3: Expr) -> DFResult<Expr> {
        self.apply_builtin(BuiltinName::Replace, vec![p0, p1, p2, p3])
    }

    pub fn replace(&self, p0: Expr, p1: Expr, p2: Expr) -> DFResult<Expr> {
        self.apply_builtin(BuiltinName::Replace, vec![p0, p1, p2])
    }

    pub fn abs(&self, val: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Abs;
        self.apply_builtin(name, vec![val])
    }

    pub fn round(&self, val: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Round;
        self.apply_builtin(name, vec![val])
    }

    pub fn ceil(&self, val: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Ceil;
        self.apply_builtin(name, vec![val])
    }

    pub fn floor(&self, val: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Floor;
        self.apply_builtin(name, vec![val])
    }

    pub fn regex_with_flags(&self, p0: Expr, p1: Expr, p2: Expr) -> DFResult<Expr> {
        self.apply_builtin(BuiltinName::Regex, vec![p0, p1, p2])
    }

    pub fn regex(&self, p0: Expr, p1: Expr) -> DFResult<Expr> {
        self.apply_builtin(BuiltinName::Regex, vec![p0, p1])
    }

    pub fn lang_matches(&self, p0: Expr, p1: Expr) -> DFResult<Expr> {
        let name = BuiltinName::LangMatches;
        self.apply_builtin(name, vec![p0, p1])
    }

    pub fn encode_for_uri(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::EncodeForUri;
        self.apply_builtin(name, vec![p0])
    }

    pub fn concat(&self, args: Vec<Expr>) -> DFResult<Expr> {
        self.apply_builtin(BuiltinName::Concat, args)
    }

    pub fn str_after(&self, p0: Expr, p1: Expr) -> DFResult<Expr> {
        let name = BuiltinName::StrAfter;
        self.apply_builtin(name, vec![p0, p1])
    }

    pub fn str_before(&self, p0: Expr, p1: Expr) -> DFResult<Expr> {
        let name = BuiltinName::StrBefore;
        self.apply_builtin(name, vec![p0, p1])
    }

    pub fn str_ends(&self, p0: Expr, p1: Expr) -> DFResult<Expr> {
        let name = BuiltinName::StrEnds;
        self.apply_builtin(name, vec![p0, p1])
    }

    pub fn contains(&self, p0: Expr, p1: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Contains;
        self.apply_builtin(name, vec![p0, p1])
    }

    pub fn str_starts(&self, p0: Expr, p1: Expr) -> DFResult<Expr> {
        let name = BuiltinName::StrStarts;
        self.apply_builtin(name, vec![p0, p1])
    }

    pub fn lcase(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::LCase;
        self.apply_builtin(name, vec![p0])
    }

    pub fn ucase(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::UCase;
        self.apply_builtin(name, vec![p0])
    }

    pub fn substr_with_length(&self, p0: Expr, p1: Expr, p2: Expr) -> DFResult<Expr> {
        self.apply_builtin(BuiltinName::SubStr, vec![p0, p1, p2])
    }

    pub fn substr(&self, p0: Expr, p1: Expr) -> DFResult<Expr> {
        let name = BuiltinName::SubStr;
        self.apply_builtin(name, vec![p0, p1])
    }

    pub fn str_len(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::StrLen;
        self.apply_builtin(name, vec![p0])
    }

    pub fn str_uuid(&self) -> DFResult<Expr> {
        let udf = self
            .registry
            .udf_factory(FunctionName::Builtin(BuiltinName::StrUuid))
            .create_with_args(HashMap::new())?;
        Ok(udf.call(vec![]))
    }

    pub fn uuid(&self) -> DFResult<Expr> {
        let udf = self
            .registry
            .udf_factory(FunctionName::Builtin(BuiltinName::Uuid))
            .create_with_args(HashMap::new())?;
        Ok(udf.call(vec![]))
    }

    pub fn str_lang(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::StrLang;
        self.apply_builtin(name, vec![p0])
    }

    pub fn str_dt(&self, p0: Expr, p1: Expr) -> DFResult<Expr> {
        let name = BuiltinName::StrDt;
        self.apply_builtin(name, vec![p0, p1])
    }

    pub fn bnode_from(&self, value: Expr) -> DFResult<Expr> {
        let name = BuiltinName::BNode;
        self.apply_builtin(name, vec![value])
    }

    pub fn bnode(&self) -> DFResult<Expr> {
        let udf = self
            .registry
            .udf_factory(FunctionName::Builtin(BuiltinName::BNode))
            .create_with_args(HashMap::new())?;
        Ok(udf.call(vec![]))
    }

    pub fn iri(&self, p0: Option<&Iri<String>>, p1: Expr) -> DFResult<Expr> {
        let mut args = HashMap::new();
        if let Some(base) = p0 {
            let literal = NamedNode::new_unchecked(base.as_str());
            args.insert("base".to_owned(), Term::from(literal));
        }

        let udf = self
            .registry
            .udf_factory(FunctionName::Builtin(BuiltinName::Iri))
            .create_with_args(args)?;
        Ok(udf.call(vec![p1]))
    }

    pub fn datatype(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Datatype;
        self.apply_builtin(name, vec![p0])
    }

    pub fn lang(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Lang;
        self.apply_builtin(name, vec![p0])
    }

    pub fn str(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Str;
        self.apply_builtin(name, vec![p0])
    }

    pub fn is_numeric(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::IsNumeric;
        self.apply_builtin(name, vec![p0])
    }

    pub fn is_literal(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::IsLiteral;
        self.apply_builtin(name, vec![p0])
    }

    /// TODO
    pub fn is_blank(&self, value: Expr) -> DFResult<Expr> {
        let name = BuiltinName::IsBlank;
        self.apply_builtin(name, vec![value])
    }

    /// TODO
    pub fn is_iri(&self, value: Expr) -> DFResult<Expr> {
        let name = BuiltinName::IsIri;
        self.apply_builtin(name, vec![value])
    }

    /// TODO
    pub fn unary_plus(&self, value: Expr) -> DFResult<Expr> {
        let name = BuiltinName::UnaryPlus;
        self.apply_builtin(name, vec![value])
    }

    /// TODO
    pub fn unary_minus(&self, value: Expr) -> DFResult<Expr> {
        let name = BuiltinName::UnaryMinus;
        self.apply_builtin(name, vec![value])
    }

    /// TODO
    pub fn add(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Add;
        self.apply_builtin(name, vec![lhs, rhs])
    }

    /// TODO
    pub fn sub(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Sub;
        self.apply_builtin(name, vec![lhs, rhs])
    }

    /// TODO
    pub fn mul(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Mul;
        self.apply_builtin(name, vec![lhs, rhs])
    }

    /// TODO
    pub fn div(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Div;
        self.apply_builtin(name, vec![lhs, rhs])
    }

    /// TODO
    pub fn and(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let lhs = self.ensure_boolean(lhs)?;
        let rhs = self.ensure_boolean(rhs)?;

        let udf = self
            .registry
            .udf_factory(FunctionName::Builtin(BuiltinName::And))
            .create_with_args(HashMap::new())
            .expect("And does not work on RDF terms");
        Ok(udf.call(vec![lhs, rhs]))
    }

    /// TODO
    pub fn or(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let lhs = self.ensure_boolean(lhs)?;
        let rhs = self.ensure_boolean(rhs)?;

        let udf = self
            .registry
            .udf_factory(FunctionName::Builtin(BuiltinName::Or))
            .create_with_args(HashMap::new())
            .expect("And does not work on RDF terms");
        Ok(udf.call(vec![lhs, rhs]))
    }

    /// TODO
    fn ensure_boolean(&self, expr: Expr) -> DFResult<Expr> {
        let (data_type, _) = expr.data_type_and_nullable(self.schema)?;
        if data_type == DataType::Boolean {
            return Ok(expr);
        }

        self.effective_boolean_value(expr)
    }

    /// TODO
    pub fn variable(&self, variable: VariableRef<'_>) -> DFResult<Expr> {
        let column = Column::new_unqualified(variable.as_str());
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

    pub fn null_literal(&self) -> DFResult<Expr> {
        let scalar = DefaultPlainTermEncoder::encode_term(ThinError::expected())?;
        Ok(lit(scalar.into_scalar_value()))
    }

    /// TODO
    pub fn equal(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Equal;
        self.apply_builtin(name, vec![lhs, rhs])
    }

    /// TODO
    pub fn greater_than(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let name = BuiltinName::GreaterThan;
        self.apply_builtin(name, vec![lhs, rhs])
    }

    /// TODO
    pub fn greater_or_equal(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let name = BuiltinName::GreaterOrEqual;
        self.apply_builtin(name, vec![lhs, rhs])
    }

    /// TODO
    pub fn less_than(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let name = BuiltinName::LessThan;
        self.apply_builtin(name, vec![lhs, rhs])
    }

    /// TODO
    pub fn less_or_equal(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let name = BuiltinName::LessOrEqual;
        self.apply_builtin(name, vec![lhs, rhs])
    }

    /// TODO
    pub fn bound(&self, expr: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Bound;
        self.apply_builtin(name, vec![expr])
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

        self.apply_builtin(BuiltinName::NativeBooleanAsTerm, vec![expr])
    }

    /// TODO
    pub fn effective_boolean_value(&self, expr: Expr) -> DFResult<Expr> {
        let name = BuiltinName::EffectiveBooleanValue;
        self.apply_builtin(name, vec![expr])
    }

    /// TODO
    pub fn same_term(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        self.apply_builtin(BuiltinName::SameTerm, vec![lhs, rhs])
    }

    /// TODO
    pub fn is_compatible(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        self.apply_builtin(BuiltinName::IsCompatible, vec![lhs, rhs])
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
        self.effective_boolean_value(self.same_term(expr, lit(literal))?)
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
    pub fn with_encoding(&self, expr: Expr, target_encoding: EncodingName) -> DFResult<Expr> {
        let actual_encoding = self.encoding(&expr)?;
        if actual_encoding == target_encoding {
            return Ok(expr);
        }

        let builtin = match target_encoding {
            EncodingName::PlainTerm => BuiltinName::WithPlainTermEncoding,
            EncodingName::TypedValue => BuiltinName::WithTypedValueEncoding,
            EncodingName::Sortable => BuiltinName::WithSortableEncoding,
        };

        let udf = self
            .registry
            .udf_factory(FunctionName::Builtin(builtin))
            .create_with_args(HashMap::new())?;
        Ok(udf.call(vec![expr]))
    }

    /// TODO
    fn apply_builtin(&self, name: BuiltinName, args: Vec<Expr>) -> DFResult<Expr> {
        let udf_factory = self.registry.udf_factory(FunctionName::Builtin(name));

        let target_encoding = udf_factory.encoding().pop().ok_or(plan_datafusion_err!(
            "The UDF factory for {} is not valid for any Encoding.",
            name
        ))?;
        let args = args
            .into_iter()
            .map(|e| self.with_encoding(e, target_encoding))
            .collect::<DFResult<Vec<_>>>()?;

        // TODO pass encoding into function
        let udf = udf_factory.create_with_args(HashMap::new())?;
        Ok(udf.call(args))
    }

    /// TODO
    fn apply_builtin_udaf(
        &self,
        name: BuiltinName,
        arg: Expr,
        distinct: bool,
        args: HashMap<String, Term>,
    ) -> DFResult<Expr> {
        // Currently, UDAFs are only supported for typed values
        let arg = self.with_encoding(arg, EncodingName::TypedValue)?;
        let udaf = self
            .registry
            .udaf_factory(FunctionName::Builtin(name))
            .create_with_args(args)?;

        Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
            udaf,
            vec![arg],
            distinct,
            None,
            None,
            None,
        )))
    }
}
