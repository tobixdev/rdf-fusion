use crate::builtin::{BuiltinName, GraphFusionUdfFactory};
use crate::scalar::unary::UnaryScalarUdfOp;
use crate::{impl_unary_sparql_op, DFResult, FunctionName};
use datafusion::common::exec_err;
use datafusion::logical_expr::ScalarUDF;
use graphfusion_encoding::typed_value::decoders::{
    DateTimeTermValueDecoder, DefaultTypedValueDecoder, NumericTermValueDecoder,
    SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use graphfusion_encoding::typed_value::encoders::{
    BlankNodeRefTermValueEncoder, BooleanTermValueEncoder, DateTimeTermValueEncoder,
    DayTimeDurationTermValueEncoder, DecimalTermValueEncoder, DoubleTermValueEncoder,
    FloatTermValueEncoder, IntTermValueEncoder, IntegerTermValueEncoder,
    NamedNodeRefTermValueEncoder, NamedNodeTermValueEncoder, NumericTypedValueEncoder,
    OwnedStringLiteralTermValueEncoder, SimpleLiteralRefTermValueEncoder,
};
use graphfusion_encoding::typed_value::TypedValueEncoding;
use graphfusion_encoding::{EncodingName, TermDecoder, TermEncoder, TermEncoding};
use graphfusion_functions_scalar::{
    AbsSparqlOp, AsDecimalSparqlOp, AsDoubleSparqlOp, AsFloatSparqlOp, AsIntSparqlOp,
    AsIntegerSparqlOp, AsStringSparqlOp, BNodeSparqlOp, BoundSparqlOp, CeilSparqlOp,
    DatatypeSparqlOp, DaySparqlOp, EncodeForUriSparqlOp, FloorSparqlOp, HoursSparqlOp, IriSparqlOp,
    IsBlankSparqlOp, IsIriSparqlOp, IsLiteralSparqlOp, IsNumericSparqlOp, LCaseSparqlOp,
    LangSparqlOp, Md5SparqlOp, MinutesSparqlOp, MonthSparqlOp, RoundSparqlOp, SecondsSparqlOp,
    Sha1SparqlOp, Sha256SparqlOp, Sha384SparqlOp, Sha512SparqlOp, StrLenSparqlOp, StrSparqlOp,
    TimezoneSparqlOp, TzSparqlOp, UCaseSparqlOp, UnaryMinusSparqlOp, UnaryPlusSparqlOp,
    YearSparqlOp,
};
use graphfusion_functions_scalar::{AsBooleanSparqlOp, AsDateTimeSparqlOp};
use graphfusion_model::{Iri, Term};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

// Conversion
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    AsBooleanTypedValueFactory,
    AsBooleanSparqlOp,
    BuiltinName::AsBoolean
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DateTimeTermValueEncoder,
    AsDateTimeTypedValueFactory,
    AsDateTimeSparqlOp,
    BuiltinName::AsDateTime
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DecimalTermValueEncoder,
    AsDecimalTypedValueFactory,
    AsDecimalSparqlOp,
    BuiltinName::AsDecimal
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DoubleTermValueEncoder,
    AsDoubleTypedValueFactory,
    AsDoubleSparqlOp,
    BuiltinName::AsDouble
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    FloatTermValueEncoder,
    AsFloatTypedValueFactory,
    AsFloatSparqlOp,
    BuiltinName::AsFloat
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    IntTermValueEncoder,
    AsIntTypedValueFactory,
    AsIntSparqlOp,
    BuiltinName::AsInt
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    IntegerTermValueEncoder,
    AsIntegerTypedValueFactory,
    AsIntegerSparqlOp,
    BuiltinName::AsInteger
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    AsStringTypedValueFactory,
    AsStringSparqlOp,
    BuiltinName::AsString
);

// Dates and Times
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    DayTypedValueFactory,
    DaySparqlOp,
    BuiltinName::Day
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    HoursTypedValueFactory,
    HoursSparqlOp,
    BuiltinName::Hours
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    MinutesTypedValueFactory,
    MinutesSparqlOp,
    BuiltinName::Minutes
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    MonthTypedValueFactory,
    MonthSparqlOp,
    BuiltinName::Month
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    DecimalTermValueEncoder,
    SecondsTypedValueFactory,
    SecondsSparqlOp,
    BuiltinName::Seconds
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    DayTimeDurationTermValueEncoder,
    TimezoneTypedValueFactory,
    TimezoneSparqlOp,
    BuiltinName::Timezone
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    TzTypedValueFactory,
    TzSparqlOp,
    BuiltinName::Tz
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    YearTypedValueFactory,
    YearSparqlOp,
    BuiltinName::Year
);

// Functional Form
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder, // For Bound
    BoundTypedValueFactory,
    BoundSparqlOp,
    BuiltinName::Bound
);

// Hashing
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    Md5TypedValueFactory,
    Md5SparqlOp,
    BuiltinName::Md5
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    Sha1TypedValueFactory,
    Sha1SparqlOp,
    BuiltinName::Sha1
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    Sha256TypedValueFactory,
    Sha256SparqlOp,
    BuiltinName::Sha256
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    Sha384TypedValueFactory,
    Sha384SparqlOp,
    BuiltinName::Sha384
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    Sha512TypedValueFactory,
    Sha512SparqlOp,
    BuiltinName::Sha512
);

// Numeric
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    AbsTypedValueFactory,
    AbsSparqlOp,
    BuiltinName::Abs
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    CeilTypedValueFactory,
    CeilSparqlOp,
    BuiltinName::Ceil
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    FloorTypedValueFactory,
    FloorSparqlOp,
    BuiltinName::Floor
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    RoundTypedValueFactory,
    RoundSparqlOp,
    BuiltinName::Round
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    UnaryMinusTypedValueFactory,
    UnaryMinusSparqlOp,
    BuiltinName::UnaryMinus
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    UnaryPlusTypedValueFactory,
    UnaryPlusSparqlOp,
    BuiltinName::UnaryPlus
);

// Strings
impl_unary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    EncodeForUriTypedValueFactory,
    EncodeForUriSparqlOp,
    BuiltinName::EncodeForUri
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    LCaseTypedValueFactory,
    LCaseSparqlOp,
    BuiltinName::LCase
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    IntegerTermValueEncoder,
    StrLenTypedValueFactory,
    StrLenSparqlOp,
    BuiltinName::StrLen
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    UCaseTypedValueFactory,
    UCaseSparqlOp,
    BuiltinName::UCase
);

// Terms
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    BlankNodeRefTermValueEncoder,
    BNodeTypedValueFactory,
    BNodeSparqlOp,
    BuiltinName::BNode
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    NamedNodeRefTermValueEncoder,
    DatatypeTypedValueFactory,
    DatatypeSparqlOp,
    BuiltinName::Datatype
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    IsBlankTypedValueFactory,
    IsBlankSparqlOp,
    BuiltinName::IsBlank
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    IsIriTypedValueFactory,
    IsIriSparqlOp,
    BuiltinName::IsIri
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    IsLiteralTypedValueFactory,
    IsLiteralSparqlOp,
    BuiltinName::IsLiteral
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    IsNumericTypedValueFactory,
    IsNumericSparqlOp,
    BuiltinName::IsNumeric
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    SimpleLiteralRefTermValueEncoder,
    LangTypedValueFactory,
    LangSparqlOp,
    BuiltinName::Lang
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    StrTypedValueFactory,
    StrSparqlOp,
    BuiltinName::Str
);

#[derive(Debug)]
struct IriBuiltinFactory {}

impl IriBuiltinFactory {
    pub const BASE_IRI: &'static str = "base_iri";
}

impl GraphFusionUdfFactory for IriBuiltinFactory {
    fn name(&self) -> FunctionName {
        FunctionName::Builtin(BuiltinName::Iri)
    }

    fn encoding(&self) -> Vec<EncodingName> {
        vec![EncodingName::TypedValue]
    }

    /// Creates a DataFusion [ScalarUDF] given the `constant_args`.
    fn create_with_args(&self, mut constant_args: HashMap<String, Term>) -> DFResult<Arc<ScalarUDF>> {
        let base_iri = extract_base_iri(&mut constant_args)?;

        let op = IriSparqlOp::new(base_iri);
        let udf_impl = UnaryScalarUdfOp::<
            IriSparqlOp,
            TypedValueEncoding,
            DefaultTypedValueDecoder,
            NamedNodeTermValueEncoder,
        >::new(self.name(), op);
        Ok(Arc::new(ScalarUDF::new_from_impl(udf_impl)))
    }
}

fn extract_base_iri(constant_args: &mut HashMap<String, Term>) -> DFResult<Option<Iri<String>>> {
    let Some(term) = constant_args.remove(IriBuiltinFactory::BASE_IRI) else {
        return Ok(None);
    };

    match term {
        Term::NamedNode(iri) => Ok(Some(Iri::parse_unchecked(iri.into_string()))),
        _ => exec_err!(
            "{} expcted an IRI for argument {}.",
            BuiltinName::Iri,
            IriBuiltinFactory::BASE_IRI
        ),
    }
}
