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
use crate::scalar::comparison::{
    EqualSparqlOp, GreaterOrEqualSparqlOp, GreaterThanSparqlOp, LessOrEqualSparqlOp,
    LessThanSparqlOp, SameTermSparqlOp,
};
use crate::scalar::conversion::CastBooleanSparqlOp;
use crate::scalar::conversion::CastDateTimeSparqlOp;
use crate::scalar::conversion::CastDecimalSparqlOp;
use crate::scalar::conversion::CastDoubleSparqlOp;
use crate::scalar::conversion::CastFloatSparqlOp;
use crate::scalar::conversion::CastIntSparqlOp;
use crate::scalar::conversion::CastIntegerSparqlOp;
use crate::scalar::conversion::CastStringSparqlOp;
use crate::scalar::dates_and_times::HoursSparqlOp;
use crate::scalar::dates_and_times::MinutesSparqlOp;
use crate::scalar::dates_and_times::MonthSparqlOp;
use crate::scalar::dates_and_times::SecondsSparqlOp;
use crate::scalar::dates_and_times::TimezoneSparqlOp;
use crate::scalar::dates_and_times::YearSparqlOp;
use crate::scalar::dates_and_times::{DaySparqlOp, TzSparqlOp};
use crate::scalar::functional_form::BoundSparqlOp;
use crate::scalar::numeric::RoundSparqlOp;
use crate::scalar::numeric::{AbsSparqlOp, UnaryMinusSparqlOp, UnaryPlusSparqlOp};
use crate::scalar::numeric::{AddSparqlOp, DivSparqlOp, FloorSparqlOp, MulSparqlOp, SubSparqlOp};
use crate::scalar::numeric::{CeilSparqlOp, RandSparqlOp};
use crate::scalar::strings::{ContainsSparqlOp, EncodeForUriSparqlOp, LCaseSparqlOp, LangMatchesSparqlOp, Md5SparqlOp, RegexSparqlOp, Sha1SparqlOp, Sha256SparqlOp, Sha384SparqlOp, Sha512SparqlOp, StrAfterSparqlOp, StrBeforeSparqlOp, StrEndsSparqlOp, StrLenSparqlOp, StrStartsSparqlOp, StrUuidSparqlOp, SubStrSparqlOp, UCaseSparqlOp};
use crate::scalar::terms::{
    BNodeSparqlOp, DatatypeSparqlOp, IriSparqlOp, IsBlankSparqlOp, IsIriSparqlOp,
    IsLiteralSparqlOp, IsNumericSparqlOp, LangSparqlOp, StrDtSparqlOp, StrLangSparqlOp,
    StrSparqlOp, UuidSparqlOp,
};
use crate::scalar::typed_value::{coalesce_typed_value, concat_typed_value, if_typed_value};
use crate::scalar::{replace, ScalarSparqlOp, ScalarSparqlOpAdapter};
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
                BuiltinName::Str => create_scalar_sparql_op::<StrSparqlOp>(),
                BuiltinName::Lang => create_scalar_sparql_op::<LangSparqlOp>(),
                BuiltinName::LangMatches => create_scalar_sparql_op::<LangMatchesSparqlOp>(),
                BuiltinName::Datatype => create_scalar_sparql_op::<DatatypeSparqlOp>(),
                BuiltinName::Iri => {
                    let iri = constant_args.get(RdfFusionBuiltinArgNames::BASE_IRI)?;
                    let op = IriSparqlOp::new(iri);
                    create_scalar_udf(op)
                }
                BuiltinName::BNode => create_scalar_sparql_op::<BNodeSparqlOp>(),
                BuiltinName::Rand => create_scalar_sparql_op::<RandSparqlOp>(),
                BuiltinName::Abs => create_scalar_sparql_op::<AbsSparqlOp>(),
                BuiltinName::Ceil => create_scalar_sparql_op::<CeilSparqlOp>(),
                BuiltinName::Floor => create_scalar_sparql_op::<FloorSparqlOp>(),
                BuiltinName::Round => create_scalar_sparql_op::<RoundSparqlOp>(),
                BuiltinName::Concat => concat_typed_value(),
                BuiltinName::SubStr => create_scalar_sparql_op::<SubStrSparqlOp>(),
                BuiltinName::StrLen => create_scalar_sparql_op::<StrLenSparqlOp>(),
                BuiltinName::Replace => replace(),
                BuiltinName::UCase => create_scalar_sparql_op::<UCaseSparqlOp>(),
                BuiltinName::LCase => create_scalar_sparql_op::<LCaseSparqlOp>(),
                BuiltinName::EncodeForUri => create_scalar_sparql_op::<EncodeForUriSparqlOp>(),
                BuiltinName::Contains => create_scalar_sparql_op::<ContainsSparqlOp>(),
                BuiltinName::StrStarts => create_scalar_sparql_op::<StrStartsSparqlOp>(),
                BuiltinName::StrEnds => create_scalar_sparql_op::<StrEndsSparqlOp>(),
                BuiltinName::StrBefore => create_scalar_sparql_op::<StrBeforeSparqlOp>(),
                BuiltinName::StrAfter => create_scalar_sparql_op::<StrAfterSparqlOp>(),
                BuiltinName::Year => create_scalar_sparql_op::<YearSparqlOp>(),
                BuiltinName::Month => create_scalar_sparql_op::<MonthSparqlOp>(),
                BuiltinName::Day => create_scalar_sparql_op::<DaySparqlOp>(),
                BuiltinName::Hours => create_scalar_sparql_op::<HoursSparqlOp>(),
                BuiltinName::Minutes => create_scalar_sparql_op::<MinutesSparqlOp>(),
                BuiltinName::Seconds => create_scalar_sparql_op::<SecondsSparqlOp>(),
                BuiltinName::Timezone => create_scalar_sparql_op::<TimezoneSparqlOp>(),
                BuiltinName::Tz => create_scalar_sparql_op::<TzSparqlOp>(),
                BuiltinName::Uuid => create_scalar_sparql_op::<UuidSparqlOp>(),
                BuiltinName::StrUuid => create_scalar_sparql_op::<StrUuidSparqlOp>(),
                BuiltinName::Md5 => create_scalar_sparql_op::<Md5SparqlOp>(),
                BuiltinName::Sha1 => create_scalar_sparql_op::<Sha1SparqlOp>(),
                BuiltinName::Sha256 => create_scalar_sparql_op::<Sha256SparqlOp>(),
                BuiltinName::Sha384 => create_scalar_sparql_op::<Sha384SparqlOp>(),
                BuiltinName::Sha512 => create_scalar_sparql_op::<Sha512SparqlOp>(),
                BuiltinName::StrLang => create_scalar_sparql_op::<StrLangSparqlOp>(),
                BuiltinName::StrDt => create_scalar_sparql_op::<StrDtSparqlOp>(),
                BuiltinName::IsIri => create_scalar_sparql_op::<IsIriSparqlOp>(),
                BuiltinName::IsBlank => create_scalar_sparql_op::<IsBlankSparqlOp>(),
                BuiltinName::IsLiteral => create_scalar_sparql_op::<IsLiteralSparqlOp>(),
                BuiltinName::IsNumeric => create_scalar_sparql_op::<IsNumericSparqlOp>(),
                BuiltinName::Regex => create_scalar_sparql_op::<RegexSparqlOp>(),
                BuiltinName::Bound => create_scalar_sparql_op::<BoundSparqlOp>(),
                BuiltinName::Coalesce => coalesce_typed_value(),
                BuiltinName::If => if_typed_value(),
                BuiltinName::SameTerm => create_scalar_sparql_op::<SameTermSparqlOp>(),
                BuiltinName::Equal => create_scalar_sparql_op::<EqualSparqlOp>(),
                BuiltinName::GreaterThan => create_scalar_sparql_op::<GreaterThanSparqlOp>(),
                BuiltinName::GreaterOrEqual => create_scalar_sparql_op::<GreaterOrEqualSparqlOp>(),
                BuiltinName::LessThan => create_scalar_sparql_op::<LessThanSparqlOp>(),
                BuiltinName::LessOrEqual => create_scalar_sparql_op::<LessOrEqualSparqlOp>(),
                BuiltinName::Add => create_scalar_sparql_op::<AddSparqlOp>(),
                BuiltinName::Div => create_scalar_sparql_op::<DivSparqlOp>(),
                BuiltinName::Mul => create_scalar_sparql_op::<MulSparqlOp>(),
                BuiltinName::Sub => create_scalar_sparql_op::<SubSparqlOp>(),
                BuiltinName::UnaryMinus => create_scalar_sparql_op::<UnaryMinusSparqlOp>(),
                BuiltinName::UnaryPlus => create_scalar_sparql_op::<UnaryPlusSparqlOp>(),
                BuiltinName::And => sparql_and(),
                BuiltinName::Or => sparql_or(),
                BuiltinName::CastString => create_scalar_sparql_op::<CastStringSparqlOp>(),
                BuiltinName::CastInteger => create_scalar_sparql_op::<CastIntegerSparqlOp>(),
                BuiltinName::AsInt => create_scalar_sparql_op::<CastIntSparqlOp>(),
                BuiltinName::CastFloat => create_scalar_sparql_op::<CastFloatSparqlOp>(),
                BuiltinName::CastDouble => create_scalar_sparql_op::<CastDoubleSparqlOp>(),
                BuiltinName::CastDecimal => create_scalar_sparql_op::<CastDecimalSparqlOp>(),
                BuiltinName::CastDateTime => create_scalar_sparql_op::<CastDateTimeSparqlOp>(),
                BuiltinName::CastBoolean => create_scalar_sparql_op::<CastBooleanSparqlOp>(),
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
    create_scalar_udf(op)
}

fn create_scalar_udf<TSparqlOp>(op: TSparqlOp) -> Arc<ScalarUDF>
where
    TSparqlOp: ScalarSparqlOp + 'static,
{
    let adapter = ScalarSparqlOpAdapter::new(op);
    Arc::new(ScalarUDF::new_from_impl(adapter))
}
