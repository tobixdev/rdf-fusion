use crate::{ActiveGraph, DFResult, RdfFusionExprBuilderRoot};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_datafusion_err, plan_err};
use datafusion::functions_aggregate::count::{count, count_distinct};
use datafusion::functions_aggregate::first_last::first_value;
use datafusion::logical_expr::{lit, Expr, ExprSchemable};
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, EncodingScalar, TermEncoding};
use rdf_fusion_functions::builtin::BuiltinName;
use rdf_fusion_functions::{
    RdfFusionBuiltinArgNames, RdfFusionFunctionArgs, RdfFusionFunctionArgsBuilder,
};
use rdf_fusion_model::{GraphName, Iri, TermRef};

/// A builder for expressions that make use of RdfFusion built-ins.
///
/// Users of RdfFusion can override all built-ins with custom implementations. As a result,
/// constructing expressions requires access to some `state` that holds the set of registered
/// built-ins. This struct provides an abstraction over using this `registry`.
#[derive(Debug, Clone)]
pub struct RdfFusionExprBuilder<'root> {
    /// Holds a reference to the factory that created this builder.
    root: RdfFusionExprBuilderRoot<'root>,
    /// The expression that is being built
    expr: Expr,
}

impl<'root> RdfFusionExprBuilder<'root> {
    /// Creates a new expression builder.
    ///
    /// Returns an `Err` if the expression does not evaluate to an RDF term.
    pub fn try_new_from_root(root: RdfFusionExprBuilderRoot<'root>, expr: Expr) -> DFResult<Self> {
        let result = Self { root, expr };
        result.encoding()?;
        Ok(result)
    }

    /// Returns the schema of the input data.
    pub fn root(&self) -> &RdfFusionExprBuilderRoot<'root> {
        &self.root
    }

    //
    // Functional Forms
    //

    /// TODO
    pub fn bound(self) -> DFResult<Self> {
        let name = BuiltinName::Bound;
        self.apply_builtin(name, Vec::new())
    }

    /// Returns an expression that evaluates to either the value of `if_true´ or `if_false`
    /// depending on the effective boolean value of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 IF](https://www.w3.org/TR/sparql11-query/#func-if)
    pub fn sparql_if(self, if_true: Expr, if_false: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::If, vec![if_true, if_false])
    }

    /// Creates a new expression that evaluates to the first argument that does not produce an
    /// error.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Coalesce](https://www.w3.org/TR/sparql11-query/#func-coalesce)
    pub fn coalesce(self, args: Vec<Expr>) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Coalesce, args)
    }

    // TODO exists

    /// TODO
    pub fn and(self: Self, rhs: Expr) -> DFResult<Self> {
        let root = self.root;
        let udf = root.create_builtin_udf(BuiltinName::And)?;

        let rhs = self.root.try_create_builder(rhs)?.ensure_boolean()?;
        let lhs = self.ensure_boolean()?.build_boolean()?;

        Ok(root.try_create_builder(udf.call(vec![lhs, rhs])))
    }

    /// TODO
    pub fn or(self, rhs: Expr) -> DFResult<Self> {
        let root = self.root;
        let udf = root.create_builtin_udf(BuiltinName::Or)?;

        let rhs = self.root.try_create_builder(rhs)?.ensure_boolean()?;
        let lhs = self.ensure_boolean()?;

        root.try_create_builder(udf.call(vec![lhs, rhs]))
    }

    /// TODO
    pub fn rdf_term_equal(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Equal, vec![rhs])
    }

    // TODO In/Not in

    //
    // Functions on RDF Terms
    //

    /// TODO
    pub fn is_iri(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::IsIri, vec![])
    }

    /// TODO
    pub fn is_blank(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::IsBlank, vec![])
    }

    /// TODO
    pub fn is_literal(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::IsLiteral, vec![])
    }

    /// TODO
    pub fn is_numeric(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::IsNumeric, vec![])
    }

    /// TODO
    pub fn str(self) -> DFResult<Self> {
        let udf = self.root.create_builtin_udf(BuiltinName::Str)?;
        self.root.try_create_builder(udf.call(vec![self.expr]))
    }

    /// TODO
    pub fn lang(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Lang, vec![])
    }

    /// TODO
    pub fn datatype(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Datatype, vec![])
    }

    /// TODO
    pub fn iri(self, base_iri: Option<&Iri<String>>) -> DFResult<Self> {
        let args = RdfFusionFunctionArgsBuilder::new()
            .with_optional_arg(
                RdfFusionBuiltinArgNames::BASE_IRI.to_owned(),
                base_iri.cloned(),
            )
            .build();
        self.apply_builtin_with_args(BuiltinName::Iri, vec![], args)
    }

    /// TODO
    pub fn bnode_from(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::BNode, vec![])
    }

    /// TODO
    pub fn strdt(self, datatype_iri: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::StrDt, vec![datatype_iri])
    }

    /// TODO
    pub fn strlang(self, lang_tag: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::StrLang, vec![lang_tag])
    }

    //
    // Functions on Strings
    //

    /// TODO
    pub fn strlen(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::StrLen, Vec::new())
    }

    /// TODO
    pub fn substr(self, starting_loc: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::SubStr, vec![starting_loc])
    }

    /// TODO
    pub fn substr_with_length(self, starting_loc: Expr, length: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::SubStr, vec![starting_loc, length])
    }

    /// TODO
    pub fn ucase(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::UCase, vec![])
    }

    /// TODO
    pub fn lcase(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::LCase, vec![])
    }

    /// TODO
    pub fn str_starts(self, arg2: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::StrStarts, vec![arg2])
    }

    /// TODO
    pub fn str_ends(self, arg2: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::StrEnds, vec![arg2])
    }

    /// TODO
    pub fn contains(self, arg2: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Contains, vec![arg2])
    }

    /// TODO
    pub fn str_before(self, arg2: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::StrBefore, vec![arg2])
    }

    /// TODO
    pub fn str_after(self, arg2: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::StrAfter, vec![arg2])
    }

    /// TODO
    pub fn encode_for_uri(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::EncodeForUri, vec![])
    }

    /// TODO
    pub fn concat(self, args: Vec<Expr>) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Concat, args)
    }

    /// TODO
    pub fn lang_matches(self, language_range: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::LangMatches, vec![language_range])
    }

    /// TODO
    pub fn regex(self, pattern: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Regex, vec![pattern])
    }

    /// TODO
    pub fn regex_with_flags(self, pattern: Expr, flags: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Regex, vec![pattern, flags])
    }

    /// Replaces all occurrences of a pattern with a given replacement.
    ///
    /// If more control about the matching behavior is required, use the [Self::replace_with_flags]
    /// operation.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Replace](https://www.w3.org/TR/sparql11-query/#func-replace)
    pub fn replace(self, pattern: Expr, replacement: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Replace, vec![pattern, replacement])
    }

    /// Replaces all occurrences of a pattern with a given replacement.
    ///
    /// In addition to the regular [Self::relace] functions, this allows providing flags used for
    /// the regex matching process.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Replace](https://www.w3.org/TR/sparql11-query/#func-replace)
    pub fn replace_with_flags(
        self,
        pattern: Expr,
        replacement: Expr,
        flags: Expr,
    ) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Replace, vec![pattern, replacement, flags])
    }

    //
    // Numeric
    //

    /// Compute the absolute value of a numeric literal.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Abs](https://www.w3.org/TR/sparql11-query/#func-abs)
    pub fn abs(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Abs, vec![])
    }

    /// Rounds the inner expression to the nearest integer.
    ///
    /// If the value is exactly between two integers, the integer closer to positive infinity is
    /// used (e.g., 0.5 -> 1).
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Round](https://www.w3.org/TR/sparql11-query/#func-round)
    pub fn round(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Round, vec![])
    }

    /// TODO
    pub fn ceil(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Ceil, vec![])
    }

    /// TODO
    pub fn floor(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Floor, vec![])
    }

    //
    // Dates & Times
    //

    /// Creates a new expression that returns the year component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Year](https://www.w3.org/TR/sparql11-query/#func-year)
    pub fn year(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Year, vec![])
    }

    /// Creates a new expression that returns the month component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Month](https://www.w3.org/TR/sparql11-query/#func-month)
    pub fn month(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Month, vec![])
    }

    /// Creates a new expression that returns the day component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Day](https://www.w3.org/TR/sparql11-query/#func-day)
    pub fn day(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Day, vec![])
    }

    /// Creates a new expression that returns the hours component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Hours](https://www.w3.org/TR/sparql11-query/#func-hours)
    pub fn hours(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Hours, vec![])
    }

    /// Creates a new expression that returns the minutes component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Minutes](https://www.w3.org/TR/sparql11-query/#func-minutes)
    pub fn minutes(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Minutes, vec![])
    }

    /// Creates a new expression that returns the seconds component of the inner expression.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Seconds](https://www.w3.org/TR/sparql11-query/#func-seconds)
    pub fn seconds(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Seconds, vec![])
    }

    /// Creates a new expression that returns the timezone of the inner expression.
    ///
    /// This returns the timezone as an `xsd:dayTimeDuration`. For a simple string representation
    /// see [Self::tz].
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Timezone](https://www.w3.org/TR/sparql11-query/#func-timezone)
    pub fn timezone(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Timezone, vec![])
    }

    /// Creates a new expression that returns the timezone of the inner expression.
    ///
    /// This returns the timezone as a simple literal. For a representation as a duration
    /// see [Self::timezone].
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Timezone](https://www.w3.org/TR/sparql11-query/#func-timezone)
    pub fn tz(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Tz, vec![])
    }

    //
    // Hash Functions
    //

    /// Creates a new expression that computes the MD5 checksum of the inner expression.
    ///
    /// The checksum is encoded as a hexadecimal string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - MD5](https://www.w3.org/TR/sparql11-query/#func-md5)
    pub fn md5(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Md5, vec![])
    }

    /// Creates a new expression that computes the SHA1 checksum of the inner expression.
    ///
    /// The checksum is encoded as a hexadecimal string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - SHA1](https://www.w3.org/TR/sparql11-query/#func-sha1)
    pub fn sha1(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Sha1, vec![])
    }

    /// Creates a new expression that computes the SHA256 checksum of the inner expression.
    ///
    /// The checksum is encoded as a hexadecimal string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - SHA256](https://www.w3.org/TR/sparql11-query/#func-sha256)
    pub fn sha256(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Sha256, vec![])
    }

    /// Creates a new expression that computes the SHA384 checksum of the inner expression.
    ///
    /// The checksum is encoded as a hexadecimal string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - SHA384](https://www.w3.org/TR/sparql11-query/#func-sha384)
    pub fn sha384(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Sha384, vec![])
    }

    /// Creates a new expression that computes the SHA512 checksum of the inner expression.
    ///
    /// The checksum is encoded as a hexadecimal string.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - SHA512](https://www.w3.org/TR/sparql11-query/#func-sha512)
    pub fn sha512(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Sha512, vec![])
    }

    //
    // Constructor Functions
    //

    /// Casts the inner expression to an `xsd:string`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn cast_string(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::CastString, vec![])
    }

    /// Casts the inner expression to an `xsd:dateTime`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn cast_date_time(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::CastDateTime, vec![])
    }

    /// Casts the inner expression to an `xsd:decimal`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn cast_decimal(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::CastDecimal, vec![])
    }

    /// Casts the inner expression to an `xsd:double`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn cast_double(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::CastDouble, vec![])
    }

    /// Casts the inner expression to an `xsd:float`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn cast_float(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::CastFloat, vec![])
    }

    /// Casts the inner expression to an `xsd:integer`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn cast_integer(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::CastInteger, vec![])
    }

    /// Casts the inner expression to an `xsd:int`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn cast_int(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::AsInt, vec![])
    }

    /// Casts the inner expression to an `xsd:boolean`.
    ///
    /// Note that this does _not_ encode the result as a native boolean array. Use
    /// [Self::build_effective_boolean_value] for this purpose
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - XPath Constructor Functions](https://www.w3.org/TR/sparql11-query/#FunctionMapping)
    pub fn cast_boolean(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::AsBoolean, vec![])
    }

    //
    // Operators
    //

    /// TODO
    pub fn unary_plus(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::UnaryPlus, vec![])
    }

    /// TODO
    pub fn unary_minus(self) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::UnaryMinus, vec![])
    }

    /// TODO
    pub fn add(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Add, vec![rhs])
    }

    /// TODO
    pub fn sub(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Sub, vec![rhs])
    }

    /// TODO
    pub fn mul(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Mul, vec![rhs])
    }

    /// TODO
    pub fn div(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Div, vec![rhs])
    }

    //
    // Comparison Operators
    //

    /// TODO
    pub fn equal(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::Equal, vec![rhs])
    }

    /// TODO
    pub fn greater_than(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::GreaterThan, vec![rhs])
    }

    /// TODO
    pub fn greater_or_equal(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::GreaterOrEqual, vec![rhs])
    }

    /// TODO
    pub fn less_than(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::LessThan, vec![rhs])
    }

    /// TODO
    pub fn less_or_equal(self, rhs: Expr) -> DFResult<Self> {
        self.apply_builtin(BuiltinName::LessOrEqual, vec![rhs])
    }

    /// TODO
    pub fn not(self) -> DFResult<Self> {
        let root = self.root;
        let ebv = self.build_effective_boolean_value()?;
        let not = Self {
            expr: Expr::Not(Box::new(ebv)),
            root,
        };
        not.native_boolean_as_term()
    }

    //
    // Built-Ins for Query Evaluation
    //

    /// TODO
    pub fn build_effective_boolean_value(self) -> DFResult<Expr> {
        let name = BuiltinName::EffectiveBooleanValue;
        self.apply_builtin(name, vec![])?.build_boolean()
    }

    /// TODO
    pub fn build_is_compatible(self, rhs: Expr) -> DFResult<Self> {
        let root = self.root;
        let udf = root.create_builtin_udf(BuiltinName::IsCompatible)?;

        let rhs = self
            .root
            .try_create_builder(rhs)?
            .with_encoding(EncodingName::PlainTerm)?
            .build()?;
        let lhs = self.with_encoding(EncodingName::PlainTerm)?.build()?;

        // TODO pass encoding into function
        root.try_create_builder(udf.call(vec![lhs, rhs]))
    }

    //
    // Functions for Building Filter Predicates
    //

    /// Filters the expression based on the `active_graph`. While, in theory, this method can be
    /// used for filtering arbitrary columns, it is only sensible for those that directly refer
    /// to the graph column in the quads table.
    pub fn filter_active_graph(self, active_graph: &ActiveGraph) -> DFResult<Option<Self>> {
        let expr = match active_graph {
            ActiveGraph::DefaultGraph => Some(self.expr.is_null()),
            ActiveGraph::Union(named_graphs) => {
                let filter = named_graphs
                    .iter()
                    .map(|name| self.clone().same_term_graph_name(name))
                    .reduce(|a, b| a?.or(b?.build_boolean()?))
                    .unwrap_or(Ok(Self {
                        expr: lit(false),
                        ..self
                    }))?;
                Some(filter.build_boolean()?)
            }
            ActiveGraph::AnyNamedGraph => Some(self.expr.is_not_null()),
            ActiveGraph::AllGraphs => None,
        };
        Ok(expr.map(|expr| Self { expr, ..self }))
    }

    /// TODO
    fn same_term_graph_name(self, graph_name: &GraphName) -> DFResult<Self> {
        match graph_name {
            GraphName::NamedNode(nn) => self.same_term_scalar(nn.into()),
            GraphName::BlankNode(bnode) => self.same_term_scalar(bnode.into()),
            GraphName::DefaultGraph => Ok(Self {
                expr: self.expr.is_null(),
                ..self
            }),
        }
    }

    /// TODO
    pub fn same_term_scalar(self, scalar: TermRef<'_>) -> DFResult<Self> {
        let encoding_name = self.encoding()?;
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
        self.build_same_term(lit(literal))
    }

    //
    // Aggregate Functions
    //

    /// TODO
    /// Creates a new aggregate expression that computes the average of the inner expression.
    ///
    /// If `distinct` is true, only distinct values are considered.
    pub fn avg(self, distinct: bool) -> DFResult<Self> {
        self.apply_builtin_udaf(BuiltinName::Avg, distinct, RdfFusionFunctionArgs::empty())
    }

    /// Creates a new aggregate expression that computes the average of the inner expression.
    ///
    /// If `distinct` is true, only distinct values are considered.
    pub fn count(self, distinct: bool) -> DFResult<Self> {
        Ok(if distinct {
            let expr = count_distinct(self.expr);
            Self { expr, ..self }
        } else {
            let expr = count(self.expr);
            Self { expr, ..self }
        })
    }

    /// Creates a new aggregate expression that computes the maximum of the inner expression.
    pub fn max(self) -> DFResult<Self> {
        self.apply_builtin_udaf(BuiltinName::Max, false, RdfFusionFunctionArgs::empty())
    }

    /// Creates a new aggregate expression that computes the minimum of the inner expression.
    pub fn min(self) -> DFResult<Self> {
        self.apply_builtin_udaf(BuiltinName::Min, false, RdfFusionFunctionArgs::empty())
    }

    /// Creates a new aggregate expression that returns any value of the inner expression.
    pub fn sample(self) -> DFResult<Self> {
        Ok(Self {
            expr: first_value(self.expr, None),
            ..self
        })
    }

    /// Creates a new aggregate expression that computes the sum of the inner expression.
    pub fn sum(self, distinct: bool) -> DFResult<Self> {
        self.apply_builtin_udaf(BuiltinName::Sum, distinct, RdfFusionFunctionArgs::empty())
    }

    /// Creates a new aggregate expression that computes the concatenation of the inner expression.
    ///
    /// The `separator` parameter can be used to use a custom separator for combining strings.
    ///
    /// If `distinct` is true, only distinct values are considered.
    pub fn group_concat(self, distinct: bool, separator: Option<&str>) -> DFResult<Self> {
        let args = RdfFusionFunctionArgsBuilder::new()
            .with_optional_arg::<String>(
                RdfFusionBuiltinArgNames::SEPARATOR.to_owned(),
                separator.map(Into::into),
            )
            .build();
        self.apply_builtin_udaf(BuiltinName::GroupConcat, distinct, args)
    }

    //
    // Encodings
    //

    /// Tries to obtain the encoding from a given expression.
    fn encoding(&self) -> DFResult<EncodingName> {
        let (data_type, _) = self.expr.data_type_and_nullable(self.root.schema())?;

        EncodingName::try_from_data_type(&data_type).ok_or(plan_datafusion_err!(
            "Expression does not have a valid RDF term encoding. Data Type: {}, Expression: {}.",
            &data_type,
            &self.expr
        ))
    }

    /// Ensures that the expression is of a certain encoding.
    ///
    /// Generally one of the following things happens:
    /// - The expression already is in the `target_encoding` and the builder itself is returns.
    /// - The expression is in another encoding and the builder tries to cast the expression.
    /// - The expression is not an RDF term and an error is returned.
    pub fn with_encoding(self, target_encoding: EncodingName) -> DFResult<Self> {
        let actual_encoding = self.encoding()?;
        if actual_encoding == target_encoding {
            return Ok(self);
        }

        let builtin = match target_encoding {
            EncodingName::PlainTerm => BuiltinName::WithPlainTermEncoding,
            EncodingName::TypedValue => BuiltinName::WithTypedValueEncoding,
            EncodingName::Sortable => BuiltinName::WithSortableEncoding,
        };

        let udf = self.root.create_builtin_udf(builtin)?;
        Ok(Self {
            expr: udf.call(vec![self.expr]),
            ..self
        })
    }

    /// TODO
    pub fn native_boolean_as_term(self) -> DFResult<Self> {
        let (data_type, _) = self.expr.data_type_and_nullable(self.root.schema())?;
        if data_type != DataType::Boolean {
            return plan_err!(
                "Expression must be Boolean for {}.",
                BuiltinName::NativeBooleanAsTerm
            );
        }

        let udf = self
            .root
            .create_builtin_udf(BuiltinName::NativeBooleanAsTerm)?;
        Ok(Self {
            expr: udf.call(vec![self.expr]),
            ..self
        })
    }

    /// TODO
    pub fn native_int64_as_term(self) -> DFResult<Expr> {
        let (data_type, _) = self.expr.data_type_and_nullable(self.root.schema())?;
        if data_type != DataType::Int64 {
            return plan_err!(
                "Expression must be an Int64 for {}.",
                BuiltinName::NativeInt64AsTerm
            );
        }

        let udf = self
            .root
            .create_builtin_udf(BuiltinName::NativeInt64AsTerm)?;
        Ok(udf.call(vec![self.expr]))
    }

    //
    // Built-Ins
    //

    fn apply_builtin_udaf(
        self,
        name: BuiltinName,
        distinct: bool,
        udaf_args: RdfFusionFunctionArgs,
    ) -> DFResult<Self> {
        self.root
            .apply_builtin_udaf(name, self.expr, distinct, udaf_args)
    }

    /// TODO
    fn apply_builtin(self, name: BuiltinName, further_args: Vec<Expr>) -> DFResult<Self> {
        self.apply_builtin_with_args(name, further_args, RdfFusionFunctionArgs::empty())
    }

    /// TODO
    fn apply_builtin_with_args(
        self,
        name: BuiltinName,
        further_args: Vec<Expr>,
        udf_args: RdfFusionFunctionArgs,
    ) -> DFResult<Self> {
        let mut args = vec![self.expr];
        args.extend(further_args);
        self.root.apply_builtin_with_args(name, args, udf_args)
    }

    //
    // Building
    //

    /// Returns the expression that has been build and checks whether it evaluates to an RDF term.
    ///
    /// If you want to build a boolean expression, see [Self::build_boolean].
    pub fn build(self) -> DFResult<Expr> {
        self.encoding()?;
        Ok(self.build_any())
    }

    /// Returns the expression that has been build without any validation.
    pub fn build_any(self) -> Expr {
        self.expr
    }

    /// TODO
    pub fn build_same_term(self, rhs: Expr) -> DFResult<Expr> {
        let args = vec![self.expr.clone(), rhs]
            .into_iter()
            .map(|e| {
                self.root
                    .try_create_builder(e)?
                    .with_encoding(EncodingName::PlainTerm)?
                    .build()
            })
            .collect::<DFResult<Vec<_>>>()?;

        let udf = self.root.create_builtin_udf(BuiltinName::SameTerm)?;
        self.root
            .try_create_builder(udf.call(args))?
            .build_effective_boolean_value()
    }

    /// Returns the expression that has been built and checks whether it evaluates to a Boolean.
    ///
    /// If you want to build an RDF term expression, see [Self::build].
    fn build_boolean(self) -> DFResult<Expr> {
        let (data_type, _) = self.expr.data_type_and_nullable(self.root.schema())?;
        if data_type != DataType::Boolean {
            return plan_err!("Expression must be Boolean.");
        }

        Ok(self.build_any())
    }

    //
    // Helper Functions
    //

    /// TODO
    fn ensure_boolean(self) -> DFResult<Expr> {
        let (data_type, _) = self.expr.data_type_and_nullable(self.root.schema())?;
        if data_type == DataType::Boolean {
            return Ok(self.build_boolean()?);
        }

        self.build_effective_boolean_value()
    }
}
