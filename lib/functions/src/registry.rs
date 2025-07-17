use crate::aggregates::{
    avg_typed_value, group_concat_typed_value, max_typed_value, min_typed_value, sum_typed_value,
};
use crate::builtin::encoding::{
    with_plain_term_encoding, with_sortable_term_encoding, with_typed_value_encoding,
};
use crate::builtin::logical::{sparql_and, sparql_or};
use crate::builtin::native::{
    effective_boolean_value, native_boolean_as_term, native_int64_as_term,
};
use crate::builtin::query::is_compatible;
use crate::builtin::BuiltinName;
use crate::scalar::conversion::as_boolean::CastBooleanSparqlOp;
use crate::scalar::conversion::as_datetime::CastDateTimeSparqlOp;
use crate::scalar::conversion::as_decimal::CastDecimalSparqlOp;
use crate::scalar::conversion::as_double::CastDoubleSparqlOp;
use crate::scalar::conversion::as_float::CastFloatSparqlOp;
use crate::scalar::conversion::as_int::CastIntSparqlOp;
use crate::scalar::conversion::as_integer::CastIntegerSparqlOp;
use crate::scalar::dates_and_times::day::DaySparqlOp;
use crate::scalar::dates_and_times::hours::HoursSparqlOp;
use crate::scalar::dates_and_times::minutes::MinutesSparqlOp;
use crate::scalar::dates_and_times::month::MonthSparqlOp;
use crate::scalar::dates_and_times::seconds::SecondsSparqlOp;
use crate::scalar::dates_and_times::timezone::TimezoneSparqlOp;
use crate::scalar::dates_and_times::year::YearSparqlOp;
use crate::scalar::numeric::abs::AbsSparqlOp;
use crate::scalar::numeric::ceil::CeilSparqlOp;
use crate::scalar::numeric::floor::FloorSparqlOp;
use crate::scalar::numeric::round::RoundSparqlOp;
use crate::scalar::plain_term::same_term;
use crate::scalar::terms::{IsBlankSparqlOp, IsIriSparqlOp, IsLiteralSparqlOp, IsNumericSparqlOp};
use crate::scalar::typed_value::{
    add_typed_value, as_string_typed_value, bound_typed_value, coalesce_typed_value,
    concat_typed_value, contains_typed_value, datatype_typed_value, div_typed_value,
    encode_for_uri_typed_value, equal_typed_value, greater_or_equal_typed_value,
    greater_than_typed_value, if_typed_value, iri_typed_value, lang_matches_typed_value,
    lang_typed_value, lcase_typed_value, less_or_equal_typed_value, less_than_typed_value,
    md5_typed_value, mul_typed_value, rand_typed_value, sha1_typed_value, sha256_typed_value,
    sha384_typed_value, sha512_typed_value, str_after_typed_value, str_before_typed_value,
    str_dt_typed_value, str_ends_typed_value, str_lang_typed_value, str_len_typed_value,
    str_starts_typed_value, str_uuid_typed_value, sub_typed_value, tz_typed_value,
    ucase_typed_value, unary_minus_typed_value, unary_plus_typed_value, uuid_typed_value,
};
use crate::scalar::{bnode, regex, replace, str, sub_str, ScalarSparqlOp, ScalarSparqlOpAdapter};
use crate::{FunctionName, RdfFusionBuiltinArgNames, RdfFusionFunctionArgs};
use datafusion::common::plan_err;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use rdf_fusion_common::DFResult;
use std::fmt::Debug;
use std::sync::Arc;

/// A reference-counted pointer to an implementation of the `RdfFusionFunctionRegistry` trait.
///
/// This type alias is used throughout the codebase to pass around references to
/// function registries without tying code to specific implementations.
pub type RdfFusionFunctionRegistryRef = Arc<dyn RdfFusionFunctionRegistry>;

/// A registry for SPARQL functions that can create DataFusion UDFs and UDAFs.
///
/// This trait defines the interface for creating DataFusion user-defined functions
/// (UDFs) and user-defined aggregate functions (UDAFs) that implement SPARQL
/// function semantics.
///
/// # Additional Resources
/// - [SPARQL 1.1 Query Language - Functions](https://www.w3.org/TR/sparql11-query/#SparqlOps)
pub trait RdfFusionFunctionRegistry: Debug + Send + Sync {
    /// Creates a DataFusion [ScalarUDF] given the `constant_args`.
    fn create_udf(
        &self,
        function_name: FunctionName,
        constant_args: RdfFusionFunctionArgs,
    ) -> DFResult<Arc<ScalarUDF>>;

    /// Creates a DataFusion [AggregateUDF] given the `constant_args`.
    fn create_udaf(
        &self,
        function_name: FunctionName,
        constant_args: RdfFusionFunctionArgs,
    ) -> DFResult<Arc<AggregateUDF>>;
}

/// The default implementation of the `RdfFusionFunctionRegistry` trait.
///
/// This registry provides implementations for all standard SPARQL functions
/// defined in the SPARQL 1.1 specification, mapping them to their corresponding
/// DataFusion UDFs and UDAFs.
///
/// # Additional Resources
/// - [SPARQL 1.1 Query Language - Function Library](https://www.w3.org/TR/sparql11-query/#SparqlOps)
#[derive(Debug, Default)]
pub struct DefaultRdfFusionFunctionRegistry;

impl RdfFusionFunctionRegistry for DefaultRdfFusionFunctionRegistry {
    fn create_udf(
        &self,
        function_name: FunctionName,
        constant_args: RdfFusionFunctionArgs,
    ) -> DFResult<Arc<ScalarUDF>> {
        match function_name {
            FunctionName::Builtin(builtin) => Ok(match builtin {
                BuiltinName::Str => str(),
                BuiltinName::Lang => lang_typed_value(),
                BuiltinName::LangMatches => lang_matches_typed_value(),
                BuiltinName::Datatype => datatype_typed_value(),
                BuiltinName::Iri => {
                    let iri = constant_args.get(RdfFusionBuiltinArgNames::BASE_IRI)?;
                    iri_typed_value(iri)
                }
                BuiltinName::BNode => bnode(),
                BuiltinName::Rand => rand_typed_value(),
                BuiltinName::Abs => create_scalar_sparql_op::<AbsSparqlOp>(),
                BuiltinName::Ceil => create_scalar_sparql_op::<CeilSparqlOp>(),
                BuiltinName::Floor => create_scalar_sparql_op::<FloorSparqlOp>(),
                BuiltinName::Round => create_scalar_sparql_op::<RoundSparqlOp>(),
                BuiltinName::Concat => concat_typed_value(),
                BuiltinName::SubStr => sub_str(),
                BuiltinName::StrLen => str_len_typed_value(),
                BuiltinName::Replace => replace(),
                BuiltinName::UCase => ucase_typed_value(),
                BuiltinName::LCase => lcase_typed_value(),
                BuiltinName::EncodeForUri => encode_for_uri_typed_value(),
                BuiltinName::Contains => contains_typed_value(),
                BuiltinName::StrStarts => str_starts_typed_value(),
                BuiltinName::StrEnds => str_ends_typed_value(),
                BuiltinName::StrBefore => str_before_typed_value(),
                BuiltinName::StrAfter => str_after_typed_value(),
                BuiltinName::Year => create_scalar_sparql_op::<YearSparqlOp>(),
                BuiltinName::Month => create_scalar_sparql_op::<MonthSparqlOp>(),
                BuiltinName::Day => create_scalar_sparql_op::<DaySparqlOp>(),
                BuiltinName::Hours => create_scalar_sparql_op::<HoursSparqlOp>(),
                BuiltinName::Minutes => create_scalar_sparql_op::<MinutesSparqlOp>(),
                BuiltinName::Seconds => create_scalar_sparql_op::<SecondsSparqlOp>(),
                BuiltinName::Timezone => create_scalar_sparql_op::<TimezoneSparqlOp>(),
                BuiltinName::Tz => tz_typed_value(),
                BuiltinName::Uuid => uuid_typed_value(),
                BuiltinName::StrUuid => str_uuid_typed_value(),
                BuiltinName::Md5 => md5_typed_value(),
                BuiltinName::Sha1 => sha1_typed_value(),
                BuiltinName::Sha256 => sha256_typed_value(),
                BuiltinName::Sha384 => sha384_typed_value(),
                BuiltinName::Sha512 => sha512_typed_value(),
                BuiltinName::StrLang => str_lang_typed_value(),
                BuiltinName::StrDt => str_dt_typed_value(),
                BuiltinName::IsIri => create_scalar_sparql_op::<IsIriSparqlOp>(),
                BuiltinName::IsBlank => create_scalar_sparql_op::<IsBlankSparqlOp>(),
                BuiltinName::IsLiteral => create_scalar_sparql_op::<IsLiteralSparqlOp>(),
                BuiltinName::IsNumeric => create_scalar_sparql_op::<IsNumericSparqlOp>(),
                BuiltinName::Regex => regex(),
                BuiltinName::Bound => bound_typed_value(),
                BuiltinName::Coalesce => coalesce_typed_value(),
                BuiltinName::If => if_typed_value(),
                BuiltinName::SameTerm => same_term(),
                BuiltinName::Equal => equal_typed_value(),
                BuiltinName::GreaterThan => greater_than_typed_value(),
                BuiltinName::GreaterOrEqual => greater_or_equal_typed_value(),
                BuiltinName::LessThan => less_than_typed_value(),
                BuiltinName::LessOrEqual => less_or_equal_typed_value(),
                BuiltinName::Add => add_typed_value(),
                BuiltinName::Div => div_typed_value(),
                BuiltinName::Mul => mul_typed_value(),
                BuiltinName::Sub => sub_typed_value(),
                BuiltinName::UnaryMinus => unary_minus_typed_value(),
                BuiltinName::UnaryPlus => unary_plus_typed_value(),
                BuiltinName::And => sparql_and(),
                BuiltinName::Or => sparql_or(),
                BuiltinName::CastString => as_string_typed_value(),
                BuiltinName::CastInteger => create_scalar_sparql_op::<CastIntegerSparqlOp>(),
                BuiltinName::AsInt => create_scalar_sparql_op::<CastIntSparqlOp>(),
                BuiltinName::CastFloat => create_scalar_sparql_op::<CastFloatSparqlOp>(),
                BuiltinName::CastDouble => create_scalar_sparql_op::<CastDoubleSparqlOp>(),
                BuiltinName::CastDecimal => create_scalar_sparql_op::<CastDecimalSparqlOp>(),
                BuiltinName::CastDateTime => create_scalar_sparql_op::<CastDateTimeSparqlOp>(),
                BuiltinName::AsBoolean => create_scalar_sparql_op::<CastBooleanSparqlOp>(),
                BuiltinName::WithSortableEncoding => with_sortable_term_encoding(),
                BuiltinName::WithTypedValueEncoding => with_typed_value_encoding(),
                BuiltinName::WithPlainTermEncoding => with_plain_term_encoding(),
                BuiltinName::EffectiveBooleanValue => effective_boolean_value(),
                BuiltinName::NativeBooleanAsTerm => native_boolean_as_term(),
                BuiltinName::IsCompatible => is_compatible(),
                BuiltinName::NativeInt64AsTerm => native_int64_as_term(),
                _ => return plan_err!("'{builtin}' is not a scalar function."),
            }),
            FunctionName::Custom(_) => plan_err!("Custom functions are not supported yet."),
        }
    }

    fn create_udaf(
        &self,
        function_name: FunctionName,
        constant_args: RdfFusionFunctionArgs,
    ) -> DFResult<Arc<AggregateUDF>> {
        match function_name {
            FunctionName::Builtin(builtin) => Ok(match builtin {
                BuiltinName::Sum => sum_typed_value(),
                BuiltinName::Min => min_typed_value(),
                BuiltinName::Max => max_typed_value(),
                BuiltinName::Avg => avg_typed_value(),
                BuiltinName::GroupConcat => {
                    let separator = constant_args.get(RdfFusionBuiltinArgNames::SEPARATOR)?;
                    group_concat_typed_value(separator)
                }
                _ => return plan_err!("'{builtin}' is not an aggregate function."),
            }),
            FunctionName::Custom(_) => plan_err!("Custom functions are not supported yet."),
        }
    }
}

fn create_scalar_sparql_op<TSparqlOp>() -> Arc<ScalarUDF>
where
    TSparqlOp: Default + ScalarSparqlOp + 'static,
{
    let op = TSparqlOp::default();
    let adapter = ScalarSparqlOpAdapter::new(op);
    Arc::new(ScalarUDF::new_from_impl(adapter))
}
