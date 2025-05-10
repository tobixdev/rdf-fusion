use crate::builtin::{BuiltinName, GraphFusionBuiltinFactory};
use crate::scalar::unary::UnaryScalarUdfOp;
use crate::{impl_unary_sparql_op, DFResult};
use datafusion::common::exec_err;
use datafusion::logical_expr::ScalarUDF;
use graphfusion_encoding::value_encoding::decoders::{
    DateTimeTermValueDecoder, DefaultTypedValueDecoder, NumericTermValueDecoder,
    SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use graphfusion_encoding::value_encoding::encoders::{
    BlankNodeRefTermValueEncoder, BooleanTermValueEncoder, DateTimeTermValueEncoder,
    DayTimeDurationTermValueEncoder, DecimalTermValueEncoder, DoubleTermValueEncoder,
    FloatTermValueEncoder, IntTermValueEncoder, IntegerTermValueEncoder,
    NamedNodeRefTermValueEncoder, NamedNodeTermValueEncoder, NumericTermValueEncoder,
    OwnedStringLiteralTermValueEncoder, SimpleLiteralRefTermValueEncoder,
};
use graphfusion_encoding::value_encoding::TypedValueEncoding;
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

// Conversion
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    AsBooleanValueUnaryDispatcher,
    AsBooleanSparqlOp,
    BuiltinName::AsBoolean
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DateTimeTermValueEncoder,
    AsDateTimeValueUnaryDispatcher,
    AsDateTimeSparqlOp,
    BuiltinName::AsDateTime
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DecimalTermValueEncoder,
    AsDecimalValueUnaryDispatcher,
    AsDecimalSparqlOp,
    BuiltinName::AsDecimal
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DoubleTermValueEncoder,
    AsDoubleValueUnaryDispatcher,
    AsDoubleSparqlOp,
    BuiltinName::AsDouble
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    FloatTermValueEncoder,
    AsFloatValueUnaryDispatcher,
    AsFloatSparqlOp,
    BuiltinName::AsFloat
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    IntTermValueEncoder,
    AsIntValueUnaryDispatcher,
    AsIntSparqlOp,
    BuiltinName::AsInt
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    IntegerTermValueEncoder,
    AsIntegerValueUnaryDispatcher,
    AsIntegerSparqlOp,
    BuiltinName::AsInteger
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    AsStringValueUnaryDispatcher,
    AsStringSparqlOp,
    BuiltinName::AsString
);

// Dates and Times
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    DayValueUnaryDispatcher,
    DaySparqlOp,
    BuiltinName::Day
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    HoursValueUnaryDispatcher,
    HoursSparqlOp,
    BuiltinName::Hours
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    MinutesValueUnaryDispatcher,
    MinutesSparqlOp,
    BuiltinName::Minutes
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    MonthValueUnaryDispatcher,
    MonthSparqlOp,
    BuiltinName::Month
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    DecimalTermValueEncoder,
    SecondsValueUnaryDispatcher,
    SecondsSparqlOp,
    BuiltinName::Seconds
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    DayTimeDurationTermValueEncoder,
    TimezoneValueUnaryDispatcher,
    TimezoneSparqlOp,
    BuiltinName::Timezone
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    TzValueUnaryDispatcher,
    TzSparqlOp,
    BuiltinName::Tz
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    YearValueUnaryDispatcher,
    YearSparqlOp,
    BuiltinName::Year
);

// Functional Form
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder, // For Bound
    BoundValueUnaryDispatcher,
    BoundSparqlOp,
    BuiltinName::Bound
);

// Hashing
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    Md5ValueUnaryDispatcher,
    Md5SparqlOp,
    BuiltinName::Md5
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    Sha1ValueUnaryDispatcher,
    Sha1SparqlOp,
    BuiltinName::Sha1
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    Sha256ValueUnaryDispatcher,
    Sha256SparqlOp,
    BuiltinName::Sha256
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    Sha384ValueUnaryDispatcher,
    Sha384SparqlOp,
    BuiltinName::Sha384
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    Sha512ValueUnaryDispatcher,
    Sha512SparqlOp,
    BuiltinName::Sha512
);

// Numeric
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    AbsValueUnaryDispatcher,
    AbsSparqlOp,
    BuiltinName::Abs
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    CeilValueUnaryDispatcher,
    CeilSparqlOp,
    BuiltinName::Ceil
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    FloorValueUnaryDispatcher,
    FloorSparqlOp,
    BuiltinName::Floor
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    RoundValueUnaryDispatcher,
    RoundSparqlOp,
    BuiltinName::Round
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    UnaryMinusValueUnaryDispatcher,
    UnaryMinusSparqlOp,
    BuiltinName::UnaryMinus
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    UnaryPlusValueUnaryDispatcher,
    UnaryPlusSparqlOp,
    BuiltinName::UnaryPlus
);

// Strings
impl_unary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    EncodeForUriValueUnaryDispatcher,
    EncodeForUriSparqlOp,
    BuiltinName::EncodeForUri
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    LCaseValueUnaryDispatcher,
    LCaseSparqlOp,
    BuiltinName::LCase
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    IntegerTermValueEncoder,
    StrLenValueUnaryDispatcher,
    StrLenSparqlOp,
    BuiltinName::StrLen
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    UCaseValueUnaryDispatcher,
    UCaseSparqlOp,
    BuiltinName::UCase
);

// Terms
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    BlankNodeRefTermValueEncoder,
    BNodeValueUnaryDispatcher,
    BNodeSparqlOp,
    BuiltinName::BNode
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    NamedNodeRefTermValueEncoder,
    DatatypeValueUnaryDispatcher,
    DatatypeSparqlOp,
    BuiltinName::Datatype
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    IsBlankValueUnaryDispatcher,
    IsBlankSparqlOp,
    BuiltinName::IsBlank
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    IsIriValueUnaryDispatcher,
    IsIriSparqlOp,
    BuiltinName::IsIri
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    IsLiteralValueUnaryDispatcher,
    IsLiteralSparqlOp,
    BuiltinName::IsLiteral
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    IsNumericValueUnaryDispatcher,
    IsNumericSparqlOp,
    BuiltinName::IsNumeric
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    SimpleLiteralRefTermValueEncoder,
    LangValueUnaryDispatcher,
    LangSparqlOp,
    BuiltinName::Lang
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    StrValueUnaryDispatcher,
    StrSparqlOp,
    BuiltinName::Str
);

#[derive(Debug)]
struct IriBuiltinFactory {}

impl IriBuiltinFactory {
    pub const BASE_IRI: &'static str = "base_iri";
}

impl GraphFusionBuiltinFactory for IriBuiltinFactory {
    fn name(&self) -> BuiltinName {
        BuiltinName::Iri
    }

    fn encoding(&self) -> Vec<EncodingName> {
        vec![EncodingName::TypedValue]
    }

    /// Creates a DataFusion [ScalarUDF] given the `constant_args`.
    fn create_with_args(&self, mut constant_args: HashMap<String, Term>) -> DFResult<ScalarUDF> {
        let base_iri = extract_base_iri(&mut constant_args)?;

        let op = IriSparqlOp::new(base_iri);
        let udf_impl = UnaryScalarUdfOp::<
            IriSparqlOp,
            TypedValueEncoding,
            DefaultTypedValueDecoder,
            NamedNodeTermValueEncoder,
        >::new(self.name(), op);
        Ok(ScalarUDF::new_from_impl(udf_impl))
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
