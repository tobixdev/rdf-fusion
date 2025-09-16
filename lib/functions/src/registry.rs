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
use datafusion::execution::FunctionRegistry;
use datafusion::execution::registry::MemoryFunctionRegistry;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use rdf_fusion_api::functions::{FunctionName, RdfFusionFunctionRegistry};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::{EncodingName, RdfFusionEncodings};
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

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
    /// A DataFusion [MemoryFunctionRegistry] that is used for actually storing the functions. Note
    /// that this registry is *not* connected to the [SessionContext] of the RDF Fusion engine.
    inner: Arc<RwLock<MemoryFunctionRegistry>>,
}

impl Debug for DefaultRdfFusionFunctionRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultRdfFusionFunctionRegistry")
            .field("encodings", &self.encodings)
            .finish()
    }
}

impl DefaultRdfFusionFunctionRegistry {
    /// Create a new [DefaultRdfFusionFunctionRegistry].
    pub fn new(encodings: RdfFusionEncodings) -> Self {
        let mut registry = Self {
            encodings,
            inner: Arc::new(RwLock::new(MemoryFunctionRegistry::new())),
        };
        register_functions(&mut registry);
        registry
    }
}

impl RdfFusionFunctionRegistry for DefaultRdfFusionFunctionRegistry {
    fn udf_supported_encodings(
        &self,
        _function_name: FunctionName,
    ) -> DFResult<Vec<EncodingName>> {
        todo!("Compute supported encodings for function. MAybe cache.")
    }

    fn udf(&self, function_name: FunctionName) -> DFResult<Arc<ScalarUDF>> {
        self.inner.read().unwrap().udf(&function_name.to_string())
    }

    fn udaf(&self, function_name: FunctionName) -> DFResult<Arc<AggregateUDF>> {
        self.inner.read().unwrap().udaf(&function_name.to_string())
    }

    fn register_udf(&self, udf: ScalarUDF) -> Option<Arc<ScalarUDF>> {
        self.inner
            .write()
            .unwrap()
            .register_udf(Arc::new(udf))
            .expect("Cannot fail")
    }

    fn register_udaf(&self, udaf: AggregateUDF) -> Option<Arc<AggregateUDF>> {
        self.inner
            .write()
            .unwrap()
            .register_udaf(Arc::new(udaf))
            .expect("Cannot fail")
    }
}

fn create_scalar_udf<TSparqlOp>(encodings: RdfFusionEncodings) -> ScalarUDF
where
    TSparqlOp: ScalarSparqlOp + 'static + Default,
{
    let adapter = ScalarSparqlOpAdapter::new(encodings, TSparqlOp::default());
    ScalarUDF::new_from_impl(adapter)
}

fn register_functions(registry: &mut DefaultRdfFusionFunctionRegistry) {
    let scalar_fns: Vec<ScalarUDF> = vec![
        create_scalar_udf::<StrSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<LangSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<LangMatchesSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<DatatypeSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<BNodeSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<RandSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<AbsSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<CeilSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<FloorSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<RoundSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<ConcatSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<SubStrSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<StrLenSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<ReplaceSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<UCaseSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<LCaseSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<EncodeForUriSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<ContainsSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<StrStartsSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<StrEndsSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<StrBeforeSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<StrAfterSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<YearSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<MonthSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<DaySparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<HoursSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<MinutesSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<SecondsSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<TimezoneSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<TzSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<UuidSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<StrUuidSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<Md5SparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<Sha1SparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<Sha256SparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<Sha384SparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<Sha512SparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<StrLangSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<StrDtSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<IsIriSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<IsBlankSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<IsLiteralSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<IsNumericSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<RegexSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<BoundSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<CoalesceSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<IfSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<SameTermSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<EqualSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<GreaterThanSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<GreaterOrEqualSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<LessThanSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<LessOrEqualSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<AddSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<DivSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<MulSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<SubSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<UnaryMinusSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<UnaryPlusSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<CastStringSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<CastIntegerSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<CastIntSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<CastFloatSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<CastDoubleSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<CastDecimalSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<CastDateTimeSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<CastBooleanSparqlOp>(registry.encodings.clone()),
        create_scalar_udf::<IriSparqlOp>(registry.encodings.clone()),
        with_sortable_term_encoding(registry.encodings.clone()),
        with_plain_term_encoding(registry.encodings.clone()),
        with_typed_value_encoding(registry.encodings.clone()),
        effective_boolean_value(),
        native_boolean_as_term(),
        is_compatible(&registry.encodings.clone()),
        native_int64_as_term(),
    ];

    for udf in scalar_fns {
        registry.register_udf(udf);
    }

    // Aggregate functions
    let aggregate_fns: Vec<AggregateUDF> = vec![
        sum_typed_value(),
        min_typed_value(),
        max_typed_value(),
        avg_typed_value(),
        group_concat_typed_value(),
    ];

    for udaf_information in aggregate_fns {
        registry.register_udaf(udaf_information);
    }
}
