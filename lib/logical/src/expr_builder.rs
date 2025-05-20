use crate::{ActiveGraph, DFResult};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_err, Column, DFSchema};
use datafusion::functions_aggregate::count::{count, count_distinct};
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{lit, Expr, ExprSchemable};
use rdf_fusion_encoding::plain_term::encoders::DefaultPlainTermEncoder;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, EncodingScalar, TermEncoder, TermEncoding};
use rdf_fusion_functions::builtin::BuiltinName;
use rdf_fusion_functions::registry::RdfFusionFunctionRegistry;
use rdf_fusion_functions::FunctionName;
use rdf_fusion_model::{GraphName, Iri, Literal, Term, TermRef, ThinError, VariableRef};
use spargebra::term::NamedNode;
use std::collections::HashMap;
use std::ops::Not;
use datafusion::functions_aggregate::first_last::first_value;

/// A builder for expressions that make use of RdfFusion built-ins.
///
/// Users of RdfFusion can override all built-ins with custom implementations. As a result,
/// constructing expressions requires access to some `state` that holds the set of registered
/// built-ins. This struct provides an abstraction over using this `registry`.
#[derive(Debug, Clone, Copy)]
pub struct RdfFusionExprBuilder<'a> {
    /// The schema of the input data. Necessary for inferring the encodings of RDF terms.
    schema: &'a DFSchema,
    /// Provides access to the builtin functions.
    registry: &'a dyn RdfFusionFunctionRegistry,
}

impl<'a> RdfFusionExprBuilder<'a> {
    /// Creates a new expression builder.
    pub fn new(schema: &'a DFSchema, registry: &'a dyn RdfFusionFunctionRegistry) -> Self {
        Self { schema, registry }
    }

    /// Returns the schema of the input data.
    pub fn schema(&self) -> &DFSchema {
        self.schema
    }

    /// Returns a reference to the used function registry.
    pub fn registry(&self) -> &dyn RdfFusionFunctionRegistry {
        self.registry
    }

    /// Creates a new aggregate expression that computes the average of the inner expression.
    ///
    /// If `distinct` is true, only distinct values are considered.
    pub fn count(&self, expr: Expr, distinct: bool) -> DFResult<Expr> {
        Ok(if distinct {
            count_distinct(expr)
        } else {
            count(expr)
        })
    }

    /// Filters the expression based on the `active_graph`. While, in theory, this method can be
    /// used for filtering arbitrary columns, it is only sensible for those that directly refer
    /// to the graph column in the quads table.
    pub fn filter_active_graph(
        &self,
        expr: Expr,
        active_graph: &ActiveGraph,
    ) -> DFResult<Option<Expr>> {
        match active_graph {
            ActiveGraph::DefaultGraph => Ok(Some(expr.is_null())),
            ActiveGraph::Union(named_graphs) => {
                let filters = named_graphs
                    .iter()
                    .map(|name| self.filter_graph_name(expr.clone(), name))
                    .collect::<DFResult<Vec<_>>>()?;
                let filter = filters
                    .into_iter()
                    .reduce(|a, b| self.or(a, b).expect("TODO"))
                    .unwrap_or(lit(false));
                Ok(Some(filter))
            }
            ActiveGraph::AnyNamedGraph => Ok(Some(expr.is_not_null())),
            ActiveGraph::AllGraphs => Ok(None),
        }
    }

    /// TODO
    fn filter_graph_name(&self, to_filter: Expr, graph_name: &GraphName) -> DFResult<Expr> {
        match graph_name {
            GraphName::NamedNode(nn) => self.filter_by_scalar(to_filter, nn.into()),
            GraphName::BlankNode(bnode) => self.filter_by_scalar(to_filter, bnode.into()),
            GraphName::DefaultGraph => Ok(to_filter.is_null()),
        }
    }

    /// TODO
    /// Creates a new aggregate expression that computes the average of the inner expression.
    ///
    /// If `distinct` is true, only distinct values are considered.
    pub fn avg(&self, expr: Expr, distinct: bool) -> DFResult<Expr> {
        self.apply_builtin_udaf(BuiltinName::Avg, expr, distinct, HashMap::new())
    }

    /// Creates a new aggregate expression that computes the maximum of the inner expression.
    pub fn max(&self, expr: Expr) -> DFResult<Expr> {
        self.apply_builtin_udaf(BuiltinName::Max, expr, false, HashMap::new())
    }

    /// Creates a new aggregate expression that computes the minimum of the inner expression.
    pub fn min(&self, expr: Expr) -> DFResult<Expr> {
        self.apply_builtin_udaf(BuiltinName::Min, expr, false, HashMap::new())
    }

    /// Creates a new aggregate expression that returns any value of the inner expression.
    pub fn sample(&self, expr: Expr) -> DFResult<Expr> {
        Ok(first_value(expr, None))
    }

    /// Creates a new aggregate expression that computes the sum of the inner expression.
    pub fn sum(&self, expr: Expr, distinct: bool) -> DFResult<Expr> {
        self.apply_builtin_udaf(BuiltinName::Sum, expr, distinct, HashMap::new())
    }

    /// Creates a new aggregate expression that computes the concatenation of the inner expression.
    ///
    /// The `separator` parameter can be used to use a custom separator for combining strings.
    ///
    /// If `distinct` is true, only distinct values are considered.
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

    /// Returns an expression that evaluates to either the value of `if_true´ or `if_false`
    /// depending on the effective boolean value of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 IF](https://www.w3.org/TR/sparql11-query/#func-if)
    pub fn sparql_if(&self, test: Expr, if_true: Expr, if_false: Expr) -> DFResult<Expr> {
        self.apply_builtin(BuiltinName::Coalesce, vec![test, if_true, if_false])
    }

    /// Creates a new expression that evaluates to the first argument that does not produce an
    /// error.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Coalesce](https://www.w3.org/TR/sparql11-query/#func-coalesce)
    pub fn coalesce(&self, args: Vec<Expr>) -> DFResult<Expr> {
        self.apply_builtin(BuiltinName::Coalesce, args)
    }

    /// Casts the inner expression to an `xsd:string`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn as_string(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::AsString;
        self.apply_builtin(name, vec![p0])
    }

    /// Casts the inner expression to an `xsd:dateTime`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn as_date_time(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::AsDateTime;
        self.apply_builtin(name, vec![p0])
    }

    /// Casts the inner expression to an `xsd:decimal`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn as_decimal(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::AsDecimal;
        self.apply_builtin(name, vec![p0])
    }

    /// Casts the inner expression to an `xsd:double`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn as_double(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::AsDouble;
        self.apply_builtin(name, vec![p0])
    }

    /// Casts the inner expression to an `xsd:float`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn as_float(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::AsFloat;
        self.apply_builtin(name, vec![p0])
    }

    /// Casts the inner expression to an `xsd:integer`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn as_integer(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::AsInteger;
        self.apply_builtin(name, vec![p0])
    }

    /// Casts the inner expression to an `xsd:int`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn as_int(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::AsInt;
        self.apply_builtin(name, vec![p0])
    }

    /// Casts the inner expression to an `xsd:boolean`.
    ///
    /// Note that this does _not_ encode the result as a native boolean array. Use
    /// [Self::effective_boolean_value] for this purpose
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn as_boolean(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::AsBoolean;
        self.apply_builtin(name, vec![p0])
    }

    /// Creates a new expression that computes the SHA512 checksum of the inner expression.
    ///
    /// The checksum is encoded as a hexadecimal string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - SHA512](https://www.w3.org/TR/sparql11-query/#func-sha512)
    pub fn sha512(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Sha512;
        self.apply_builtin(name, vec![p0])
    }

    /// Creates a new expression that computes the SHA384 checksum of the inner expression.
    ///
    /// The checksum is encoded as a hexadecimal string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - SHA384](https://www.w3.org/TR/sparql11-query/#func-sha384)
    pub fn sha384(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Sha384;
        self.apply_builtin(name, vec![p0])
    }

    /// Creates a new expression that computes the SHA256 checksum of the inner expression.
    ///
    /// The checksum is encoded as a hexadecimal string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - SHA256](https://www.w3.org/TR/sparql11-query/#func-sha256)
    pub fn sha256(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Sha256;
        self.apply_builtin(name, vec![p0])
    }

    /// Creates a new expression that computes the SHA1 checksum of the inner expression.
    ///
    /// The checksum is encoded as a hexadecimal string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - SHA1](https://www.w3.org/TR/sparql11-query/#func-sha1)
    pub fn sha1(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Sha1;
        self.apply_builtin(name, vec![p0])
    }

    /// Creates a new expression that computes the MD5 checksum of the inner expression.
    ///
    /// The checksum is encoded as a hexadecimal string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - MD5](https://www.w3.org/TR/sparql11-query/#func-md5)
    pub fn md5(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Md5;
        self.apply_builtin(name, vec![p0])
    }

    /// Creates a new expression that returns the timezone of the inner expression.
    ///
    /// This returns the timezone as a simple literal. For a representation as a duration
    /// see [Self::timezone].
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Timezone](https://www.w3.org/TR/sparql11-query/#func-timezone)
    pub fn tz(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Tz;
        self.apply_builtin(name, vec![p0])
    }

    /// Creates a new expression that returns the timezone of the inner expression.
    ///
    /// This returns the timezone as an `xsd:dayTimeDuration`. For a simple string representation
    /// see [Self::tz].
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Timezone](https://www.w3.org/TR/sparql11-query/#func-timezone)
    pub fn timezone(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Timezone;
        self.apply_builtin(name, vec![p0])
    }

    /// Creates a new expression that returns the seconds component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Seconds](https://www.w3.org/TR/sparql11-query/#func-seconds)
    pub fn seconds(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Seconds;
        self.apply_builtin(name, vec![p0])
    }

    /// Creates a new expression that returns the minutes component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Minutes](https://www.w3.org/TR/sparql11-query/#func-minutes)
    pub fn minutes(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Minutes;
        self.apply_builtin(name, vec![p0])
    }

    /// Creates a new expression that returns the hours component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Hours](https://www.w3.org/TR/sparql11-query/#func-hours)
    pub fn hours(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Hours;
        self.apply_builtin(name, vec![p0])
    }

    /// Creates a new expression that returns the day component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Day](https://www.w3.org/TR/sparql11-query/#func-day)
    pub fn day(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Day;
        self.apply_builtin(name, vec![p0])
    }

    /// Creates a new expression that returns the month component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Month](https://www.w3.org/TR/sparql11-query/#func-month)
    pub fn month(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Month;
        self.apply_builtin(name, vec![p0])
    }

    /// Creates a new expression that returns the year component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Year](https://www.w3.org/TR/sparql11-query/#func-year)
    pub fn year(&self, p0: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Year;
        self.apply_builtin(name, vec![p0])
    }

    /// Creates an expression that computes a pseudo-random value between 0 and 1.
    ///
    /// The data type of the value is `xsd:double`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Rand](https://www.w3.org/TR/sparql11-query/#idp2130040)
    pub fn rand(&self) -> DFResult<Expr> {
        let udf = self
            .registry
            .create_udf(FunctionName::Builtin(BuiltinName::Rand), HashMap::new())?;
        Ok(udf.call(vec![]))
    }

    /// Replaces all occurrences of a pattern with a given replacement.
    ///
    /// In addition to the regular [Self::relace] functions, this allows providing flags used for
    /// the regex matching process.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Replace](https://www.w3.org/TR/sparql11-query/#func-replace)
    pub fn replace_with_flags(&self, p0: Expr, p1: Expr, p2: Expr, p3: Expr) -> DFResult<Expr> {
        self.apply_builtin(BuiltinName::Replace, vec![p0, p1, p2, p3])
    }

    /// Replaces all occurrences of a pattern with a given replacement.
    ///
    /// If more control about the matching behavior is required, use the [Self::replace_with_flags]
    /// operation.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Replace](https://www.w3.org/TR/sparql11-query/#func-replace)
    pub fn replace(&self, p0: Expr, p1: Expr, p2: Expr) -> DFResult<Expr> {
        self.apply_builtin(BuiltinName::Replace, vec![p0, p1, p2])
    }

    /// Compute the absolute value of a numeric literal.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Abs](https://www.w3.org/TR/sparql11-query/#func-abs)
    pub fn abs(&self, val: Expr) -> DFResult<Expr> {
        let name = BuiltinName::Abs;
        self.apply_builtin(name, vec![val])
    }

    /// Rounds the inner expression to the nearest integer.
    ///
    /// If the value is exactly between two integers, the integer closer to positive infinity is
    /// used (e.g., 0.5 -> 1).
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Round](https://www.w3.org/TR/sparql11-query/#func-round)
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
            .create_udf(FunctionName::Builtin(BuiltinName::StrUuid), HashMap::new())?;
        Ok(udf.call(vec![]))
    }

    pub fn uuid(&self) -> DFResult<Expr> {
        let udf = self
            .registry
            .create_udf(FunctionName::Builtin(BuiltinName::Uuid), HashMap::new())?;
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
            .create_udf(FunctionName::Builtin(BuiltinName::BNode), HashMap::new())?;
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
            .create_udf(FunctionName::Builtin(BuiltinName::Iri), HashMap::new())?;
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

    pub fn str(&self, arg: Expr) -> DFResult<Expr> {
        let udf = self
            .registry
            .create_udf(FunctionName::Builtin(BuiltinName::Str), HashMap::new())?;
        Ok(udf.call(vec![arg]))
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
            .create_udf(FunctionName::Builtin(BuiltinName::And), HashMap::new())
            .expect("And does not work on RDF terms");
        Ok(udf.call(vec![lhs, rhs]))
    }

    /// TODO
    pub fn or(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let lhs = self.ensure_boolean(lhs)?;
        let rhs = self.ensure_boolean(rhs)?;

        let udf = self
            .registry
            .create_udf(FunctionName::Builtin(BuiltinName::Or), HashMap::new())
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

    /// TODO
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

        let udf = self.registry.create_udf(
            FunctionName::Builtin(BuiltinName::NativeBooleanAsTerm),
            HashMap::new(),
        )?;
        Ok(udf.call(vec![expr]))
    }

    /// TODO
    pub fn native_int64_as_term(&self, expr: Expr) -> DFResult<Expr> {
        let (data_type, _) = expr.data_type_and_nullable(self.schema)?;
        if data_type != DataType::Int64 {
            return plan_err!(
                "Expression must be an Int64 for {}.",
                BuiltinName::NativeInt64AsTerm
            );
        }

        let udf = self.registry.create_udf(
            FunctionName::Builtin(BuiltinName::NativeInt64AsTerm),
            HashMap::new(),
        )?;
        Ok(udf.call(vec![expr]))
    }

    /// TODO
    pub fn effective_boolean_value(&self, expr: Expr) -> DFResult<Expr> {
        let name = BuiltinName::EffectiveBooleanValue;
        self.apply_builtin(name, vec![expr])
    }

    /// TODO
    pub fn same_term(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let args = vec![lhs, rhs]
            .into_iter()
            .map(|e| self.with_encoding(e, EncodingName::PlainTerm))
            .collect::<DFResult<Vec<_>>>()?;

        let udf = self
            .registry
            .create_udf(FunctionName::Builtin(BuiltinName::SameTerm), HashMap::new())?;
        Ok(udf.call(args))
    }

    /// TODO
    pub fn is_compatible(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let lhs = self.with_encoding(lhs, EncodingName::PlainTerm)?;
        let rhs = self.with_encoding(rhs, EncodingName::PlainTerm)?;

        // TODO pass encoding into function
        let udf = self.registry.create_udf(
            FunctionName::Builtin(BuiltinName::IsCompatible),
            HashMap::new(),
        )?;
        Ok(udf.call(vec![lhs, rhs]))
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
            .create_udf(FunctionName::Builtin(builtin), HashMap::new())?;
        Ok(udf.call(vec![expr]))
    }

    /// TODO
    fn apply_builtin(&self, name: BuiltinName, args: Vec<Expr>) -> DFResult<Expr> {
        let target_encoding = TypedValueEncoding::name();
        let args = args
            .into_iter()
            .map(|e| self.with_encoding(e, target_encoding))
            .collect::<DFResult<Vec<_>>>()?;

        let udf = self
            .registry
            .create_udf(FunctionName::Builtin(name), HashMap::new())?;
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
            .create_udaf(FunctionName::Builtin(name), args)?;

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
