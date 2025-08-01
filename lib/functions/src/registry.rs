use crate::aggregates::{
    avg_typed_value, group_concat_typed_value, max_typed_value, min_typed_value,
    sum_typed_value,
};
use crate::builtin::encoding::{
    with_plain_term_encoding, with_sortable_term_encoding, with_typed_value_encoding,
};
use crate::builtin::native::{
    effective_boolean_value, native_boolean_as_term, native_int64_as_term,
};
use crate::builtin::query::is_compatible;
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
use crate::scalar::functional_form::{BoundSparqlOp, CoalesceSparqlOp, IfSparqlOp};
use crate::scalar::logical::{NotSparqlOp, sparql_and, sparql_or};
use crate::scalar::numeric::RoundSparqlOp;
use crate::scalar::numeric::{AbsSparqlOp, UnaryMinusSparqlOp, UnaryPlusSparqlOp};
use crate::scalar::numeric::{
    AddSparqlOp, DivSparqlOp, FloorSparqlOp, MulSparqlOp, SubSparqlOp,
};
use crate::scalar::numeric::{CeilSparqlOp, RandSparqlOp};
use crate::scalar::strings::{
    ConcatSparqlOp, ContainsSparqlOp, EncodeForUriSparqlOp, LCaseSparqlOp,
    LangMatchesSparqlOp, Md5SparqlOp, RegexSparqlOp, ReplaceSparqlOp, Sha1SparqlOp,
    Sha256SparqlOp, Sha384SparqlOp, Sha512SparqlOp, StrAfterSparqlOp, StrBeforeSparqlOp,
    StrEndsSparqlOp, StrLenSparqlOp, StrStartsSparqlOp, StrUuidSparqlOp, SubStrSparqlOp,
    UCaseSparqlOp,
};
use crate::scalar::terms::{
    BNodeSparqlOp, DatatypeSparqlOp, IriSparqlOp, IsBlankSparqlOp, IsIriSparqlOp,
    IsLiteralSparqlOp, IsNumericSparqlOp, LangSparqlOp, StrDtSparqlOp, StrLangSparqlOp,
    StrSparqlOp, UuidSparqlOp,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpAdapter};
use datafusion::common::{HashMap, plan_err};
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use rdf_fusion_api::functions::{
    BuiltinName, FunctionName, RdfFusionBuiltinArgNames, RdfFusionFunctionArgs,
    RdfFusionFunctionRegistry,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::{EncodingName, RdfFusionEncodings};
use std::fmt::Debug;
use std::sync::Arc;

type ScalarUdfFactory =
    Box<dyn Fn(RdfFusionFunctionArgs) -> DFResult<Arc<ScalarUDF>> + Send + Sync>;
type AggregateUdfFactory =
    Box<dyn Fn(RdfFusionFunctionArgs) -> DFResult<Arc<AggregateUDF>> + Send + Sync>;

/// The default implementation of the `RdfFusionFunctionRegistry` trait.
///
/// This registry provides implementations for all standard SPARQL functions
/// defined in the SPARQL 1.1 specification, mapping them to their corresponding
/// DataFusion UDFs and UDAFs.
///
/// # Additional Resources
/// - [SPARQL 1.1 Query Language - Function Library](https://www.w3.org/TR/sparql11-query/#SparqlOps)
pub struct DefaultRdfFusionFunctionRegistry {
    /// The registered encodings.
    encodings: RdfFusionEncodings,
    /// The mapping used for scalar functions.
    scalar_mapping: HashMap<FunctionName, (ScalarUdfFactory, Vec<EncodingName>)>,
    /// The mapping used for aggregate functions.
    aggregate_mapping: HashMap<FunctionName, (AggregateUdfFactory, Vec<EncodingName>)>,
}

impl Debug for DefaultRdfFusionFunctionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultRdfFusionFunctionRegistry")
            .field("encodings", &self.encodings)
            .field("scalar_mapping", &self.scalar_mapping.keys())
            .field("aggregate_mapping", &self.aggregate_mapping.keys())
            .finish()
    }
}

impl DefaultRdfFusionFunctionRegistry {
    /// Create a new [DefaultRdfFusionFunctionRegistry].
    pub fn new(encodings: RdfFusionEncodings) -> Self {
        let mut registry = Self {
            encodings,
            aggregate_mapping: HashMap::default(),
            scalar_mapping: HashMap::default(),
        };
        register_functions(&mut registry);
        registry
    }

    fn create_scalar_sparql_op<TSparqlOp>(&self) -> (ScalarUdfFactory, Vec<EncodingName>)
    where
        TSparqlOp: Default + ScalarSparqlOp + 'static,
    {
        create_scalar_sparql_op::<TSparqlOp>(self.encodings.clone())
    }
}

impl RdfFusionFunctionRegistry for DefaultRdfFusionFunctionRegistry {
    fn supported_encodings(
        &self,
        function_name: FunctionName,
    ) -> DFResult<Vec<EncodingName>> {
        if let Some((_, encodings)) = self.scalar_mapping.get(&function_name) {
            Ok(encodings.clone())
        } else {
            plan_err!("Could not find encodings for function '{function_name}'.")
        }
    }

    fn create_udf(
        &self,
        function_name: FunctionName,
        constant_args: RdfFusionFunctionArgs,
    ) -> DFResult<Arc<ScalarUDF>> {
        if let Some((factory, _)) = self.scalar_mapping.get(&function_name) {
            factory(constant_args)
        } else {
            plan_err!("Scalar function '{function_name}' not found.")
        }
    }

    fn create_udaf(
        &self,
        function_name: FunctionName,
        constant_args: RdfFusionFunctionArgs,
    ) -> DFResult<Arc<AggregateUDF>> {
        if let Some((factory, _)) = self.aggregate_mapping.get(&function_name) {
            factory(constant_args)
        } else {
            plan_err!("Aggregate function '{function_name}' not found.")
        }
    }
}

fn supported_encodings<TSparqlOp>(encodings: RdfFusionEncodings) -> Vec<EncodingName>
where
    TSparqlOp: Default + ScalarSparqlOp + 'static,
{
    let op = TSparqlOp::default();

    let mut result = Vec::new();
    if op.plain_term_encoding_op().is_some() {
        result.push(EncodingName::PlainTerm);
    }
    if op.typed_value_encoding_op().is_some() {
        result.push(EncodingName::TypedValue);
    }
    if let Some(oid_encoding) = encodings.object_id() {
        if op.object_id_encoding_op(oid_encoding).is_some() {
            result.push(EncodingName::ObjectId);
        }
    }

    result
}

fn create_scalar_sparql_op<TSparqlOp>(
    encodings: RdfFusionEncodings,
) -> (ScalarUdfFactory, Vec<EncodingName>)
where
    TSparqlOp: Default + ScalarSparqlOp + 'static,
{
    let udf = create_scalar_udf(encodings.clone(), TSparqlOp::default());
    let encodings = supported_encodings::<TSparqlOp>(encodings);
    let factory = Box::new(move |_| Ok(Arc::clone(&udf)));
    (factory, encodings)
}

fn create_scalar_udf<TSparqlOp>(
    encodings: RdfFusionEncodings,
    op: TSparqlOp,
) -> Arc<ScalarUDF>
where
    TSparqlOp: ScalarSparqlOp + 'static,
{
    let adapter = ScalarSparqlOpAdapter::new(encodings, op);
    Arc::new(ScalarUDF::new_from_impl(adapter))
}

fn register_functions(registry: &mut DefaultRdfFusionFunctionRegistry) {
    let encodings1 = registry.encodings.clone();
    let encodings2 = registry.encodings.clone();
    let scalar_fns: Vec<(BuiltinName, (ScalarUdfFactory, Vec<EncodingName>))> = vec![
        (
            BuiltinName::Str,
            registry.create_scalar_sparql_op::<StrSparqlOp>(),
        ),
        (
            BuiltinName::Lang,
            registry.create_scalar_sparql_op::<LangSparqlOp>(),
        ),
        (
            BuiltinName::LangMatches,
            registry.create_scalar_sparql_op::<LangMatchesSparqlOp>(),
        ),
        (
            BuiltinName::Datatype,
            registry.create_scalar_sparql_op::<DatatypeSparqlOp>(),
        ),
        (
            BuiltinName::BNode,
            registry.create_scalar_sparql_op::<BNodeSparqlOp>(),
        ),
        (
            BuiltinName::Rand,
            registry.create_scalar_sparql_op::<RandSparqlOp>(),
        ),
        (
            BuiltinName::Abs,
            registry.create_scalar_sparql_op::<AbsSparqlOp>(),
        ),
        (
            BuiltinName::Ceil,
            registry.create_scalar_sparql_op::<CeilSparqlOp>(),
        ),
        (
            BuiltinName::Floor,
            registry.create_scalar_sparql_op::<FloorSparqlOp>(),
        ),
        (
            BuiltinName::Round,
            registry.create_scalar_sparql_op::<RoundSparqlOp>(),
        ),
        (
            BuiltinName::Concat,
            registry.create_scalar_sparql_op::<ConcatSparqlOp>(),
        ),
        (
            BuiltinName::SubStr,
            registry.create_scalar_sparql_op::<SubStrSparqlOp>(),
        ),
        (
            BuiltinName::StrLen,
            registry.create_scalar_sparql_op::<StrLenSparqlOp>(),
        ),
        (
            BuiltinName::Replace,
            registry.create_scalar_sparql_op::<ReplaceSparqlOp>(),
        ),
        (
            BuiltinName::UCase,
            registry.create_scalar_sparql_op::<UCaseSparqlOp>(),
        ),
        (
            BuiltinName::LCase,
            registry.create_scalar_sparql_op::<LCaseSparqlOp>(),
        ),
        (
            BuiltinName::EncodeForUri,
            registry.create_scalar_sparql_op::<EncodeForUriSparqlOp>(),
        ),
        (
            BuiltinName::Contains,
            registry.create_scalar_sparql_op::<ContainsSparqlOp>(),
        ),
        (
            BuiltinName::StrStarts,
            registry.create_scalar_sparql_op::<StrStartsSparqlOp>(),
        ),
        (
            BuiltinName::StrEnds,
            registry.create_scalar_sparql_op::<StrEndsSparqlOp>(),
        ),
        (
            BuiltinName::StrBefore,
            registry.create_scalar_sparql_op::<StrBeforeSparqlOp>(),
        ),
        (
            BuiltinName::StrAfter,
            registry.create_scalar_sparql_op::<StrAfterSparqlOp>(),
        ),
        (
            BuiltinName::Year,
            registry.create_scalar_sparql_op::<YearSparqlOp>(),
        ),
        (
            BuiltinName::Month,
            registry.create_scalar_sparql_op::<MonthSparqlOp>(),
        ),
        (
            BuiltinName::Day,
            registry.create_scalar_sparql_op::<DaySparqlOp>(),
        ),
        (
            BuiltinName::Hours,
            registry.create_scalar_sparql_op::<HoursSparqlOp>(),
        ),
        (
            BuiltinName::Minutes,
            registry.create_scalar_sparql_op::<MinutesSparqlOp>(),
        ),
        (
            BuiltinName::Seconds,
            registry.create_scalar_sparql_op::<SecondsSparqlOp>(),
        ),
        (
            BuiltinName::Timezone,
            registry.create_scalar_sparql_op::<TimezoneSparqlOp>(),
        ),
        (
            BuiltinName::Tz,
            registry.create_scalar_sparql_op::<TzSparqlOp>(),
        ),
        (
            BuiltinName::Uuid,
            registry.create_scalar_sparql_op::<UuidSparqlOp>(),
        ),
        (
            BuiltinName::StrUuid,
            registry.create_scalar_sparql_op::<StrUuidSparqlOp>(),
        ),
        (
            BuiltinName::Md5,
            registry.create_scalar_sparql_op::<Md5SparqlOp>(),
        ),
        (
            BuiltinName::Sha1,
            registry.create_scalar_sparql_op::<Sha1SparqlOp>(),
        ),
        (
            BuiltinName::Sha256,
            registry.create_scalar_sparql_op::<Sha256SparqlOp>(),
        ),
        (
            BuiltinName::Sha384,
            registry.create_scalar_sparql_op::<Sha384SparqlOp>(),
        ),
        (
            BuiltinName::Sha512,
            registry.create_scalar_sparql_op::<Sha512SparqlOp>(),
        ),
        (
            BuiltinName::StrLang,
            registry.create_scalar_sparql_op::<StrLangSparqlOp>(),
        ),
        (
            BuiltinName::StrDt,
            registry.create_scalar_sparql_op::<StrDtSparqlOp>(),
        ),
        (
            BuiltinName::IsIri,
            registry.create_scalar_sparql_op::<IsIriSparqlOp>(),
        ),
        (
            BuiltinName::IsBlank,
            registry.create_scalar_sparql_op::<IsBlankSparqlOp>(),
        ),
        (
            BuiltinName::IsLiteral,
            registry.create_scalar_sparql_op::<IsLiteralSparqlOp>(),
        ),
        (
            BuiltinName::IsNumeric,
            registry.create_scalar_sparql_op::<IsNumericSparqlOp>(),
        ),
        (
            BuiltinName::Regex,
            registry.create_scalar_sparql_op::<RegexSparqlOp>(),
        ),
        (
            BuiltinName::Bound,
            registry.create_scalar_sparql_op::<BoundSparqlOp>(),
        ),
        (
            BuiltinName::Coalesce,
            registry.create_scalar_sparql_op::<CoalesceSparqlOp>(),
        ),
        (
            BuiltinName::If,
            registry.create_scalar_sparql_op::<IfSparqlOp>(),
        ),
        (
            BuiltinName::SameTerm,
            registry.create_scalar_sparql_op::<SameTermSparqlOp>(),
        ),
        (
            BuiltinName::Equal,
            registry.create_scalar_sparql_op::<EqualSparqlOp>(),
        ),
        (
            BuiltinName::GreaterThan,
            registry.create_scalar_sparql_op::<GreaterThanSparqlOp>(),
        ),
        (
            BuiltinName::GreaterOrEqual,
            registry.create_scalar_sparql_op::<GreaterOrEqualSparqlOp>(),
        ),
        (
            BuiltinName::LessThan,
            registry.create_scalar_sparql_op::<LessThanSparqlOp>(),
        ),
        (
            BuiltinName::LessOrEqual,
            registry.create_scalar_sparql_op::<LessOrEqualSparqlOp>(),
        ),
        (
            BuiltinName::Add,
            registry.create_scalar_sparql_op::<AddSparqlOp>(),
        ),
        (
            BuiltinName::Div,
            registry.create_scalar_sparql_op::<DivSparqlOp>(),
        ),
        (
            BuiltinName::Mul,
            registry.create_scalar_sparql_op::<MulSparqlOp>(),
        ),
        (
            BuiltinName::Sub,
            registry.create_scalar_sparql_op::<SubSparqlOp>(),
        ),
        (
            BuiltinName::UnaryMinus,
            registry.create_scalar_sparql_op::<UnaryMinusSparqlOp>(),
        ),
        (
            BuiltinName::UnaryPlus,
            registry.create_scalar_sparql_op::<UnaryPlusSparqlOp>(),
        ),
        (BuiltinName::And, (Box::new(|_| Ok(sparql_and())), vec![])),
        (BuiltinName::Or, (Box::new(|_| Ok(sparql_or())), vec![])),
        (
            BuiltinName::Not,
            registry.create_scalar_sparql_op::<NotSparqlOp>(),
        ),
        (
            BuiltinName::CastString,
            registry.create_scalar_sparql_op::<CastStringSparqlOp>(),
        ),
        (
            BuiltinName::CastInteger,
            registry.create_scalar_sparql_op::<CastIntegerSparqlOp>(),
        ),
        (
            BuiltinName::AsInt,
            registry.create_scalar_sparql_op::<CastIntSparqlOp>(),
        ),
        (
            BuiltinName::CastFloat,
            registry.create_scalar_sparql_op::<CastFloatSparqlOp>(),
        ),
        (
            BuiltinName::CastDouble,
            registry.create_scalar_sparql_op::<CastDoubleSparqlOp>(),
        ),
        (
            BuiltinName::CastDecimal,
            registry.create_scalar_sparql_op::<CastDecimalSparqlOp>(),
        ),
        (
            BuiltinName::CastDateTime,
            registry.create_scalar_sparql_op::<CastDateTimeSparqlOp>(),
        ),
        (
            BuiltinName::CastBoolean,
            registry.create_scalar_sparql_op::<CastBooleanSparqlOp>(),
        ),
        (
            BuiltinName::WithSortableEncoding,
            (
                Box::new(move |_| Ok(with_sortable_term_encoding(encodings1.clone()))),
                vec![EncodingName::PlainTerm, EncodingName::TypedValue],
            ),
        ),
        (
            BuiltinName::WithTypedValueEncoding,
            (
                Box::new(move |_| Ok(with_typed_value_encoding(encodings2.clone()))),
                vec![EncodingName::PlainTerm],
            ),
        ),
        (
            BuiltinName::EffectiveBooleanValue,
            (
                Box::new(|_| Ok(effective_boolean_value())),
                vec![EncodingName::TypedValue],
            ),
        ),
        (
            BuiltinName::NativeBooleanAsTerm,
            (Box::new(|_| Ok(native_boolean_as_term())), vec![]),
        ),
        (
            BuiltinName::IsCompatible,
            (
                Box::new(move |_| Ok(is_compatible())),
                vec![EncodingName::PlainTerm, EncodingName::ObjectId],
            ),
        ),
        (
            BuiltinName::NativeInt64AsTerm,
            (Box::new(|_| Ok(native_int64_as_term())), vec![]),
        ),
    ];

    for (name, (factory, encodings)) in scalar_fns {
        registry
            .scalar_mapping
            .insert(FunctionName::Builtin(name), (factory, encodings));
    }

    // Stateful functions
    let encodings = registry.encodings.clone();
    let iri_factory = Box::new(move |args: RdfFusionFunctionArgs| {
        let iri = args.get(RdfFusionBuiltinArgNames::BASE_IRI)?;
        let op = IriSparqlOp::new(iri);
        Ok(create_scalar_udf(encodings.clone(), op))
    });
    registry.scalar_mapping.insert(
        FunctionName::Builtin(BuiltinName::Iri),
        (iri_factory, vec![EncodingName::TypedValue]),
    );

    let encodings = registry.encodings.clone();
    let plain_term_factory =
        Box::new(move |_| Ok(with_plain_term_encoding(encodings.clone())));
    registry.scalar_mapping.insert(
        FunctionName::Builtin(BuiltinName::WithPlainTermEncoding),
        (plain_term_factory, vec![]),
    );

    // Aggregate functions
    let aggregate_fns: Vec<(BuiltinName, (AggregateUdfFactory, Vec<EncodingName>))> = vec![
        (
            BuiltinName::Sum,
            (
                Box::new(|_| Ok(sum_typed_value())),
                vec![EncodingName::TypedValue],
            ),
        ),
        (
            BuiltinName::Min,
            (
                Box::new(|_| Ok(min_typed_value())),
                vec![EncodingName::TypedValue],
            ),
        ),
        (
            BuiltinName::Max,
            (
                Box::new(|_| Ok(max_typed_value())),
                vec![EncodingName::TypedValue],
            ),
        ),
        (
            BuiltinName::Avg,
            (
                Box::new(|_| Ok(avg_typed_value())),
                vec![EncodingName::TypedValue],
            ),
        ),
        (
            BuiltinName::GroupConcat,
            (
                Box::new(|args| {
                    let separator = args.get(RdfFusionBuiltinArgNames::SEPARATOR)?;
                    Ok(group_concat_typed_value(separator))
                }),
                vec![EncodingName::TypedValue],
            ),
        ),
    ];

    for (name, udaf_information) in aggregate_fns {
        registry
            .aggregate_mapping
            .insert(FunctionName::Builtin(name), udaf_information);
    }
}
