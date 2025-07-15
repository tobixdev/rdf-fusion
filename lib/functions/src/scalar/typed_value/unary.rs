use crate::builtin::BuiltinName;
use crate::scalar::unary::UnaryScalarUdfOp;
use crate::{impl_unary_sparql_op, FunctionName};
use datafusion::logical_expr::ScalarUDF;
use rdf_fusion_encoding::typed_value::decoders::{
    DateTimeTermValueDecoder, DefaultTypedValueDecoder, NumericTermValueDecoder,
    SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use rdf_fusion_encoding::typed_value::encoders::{
    BlankNodeRefTermValueEncoder, BooleanTermValueEncoder, DateTimeTermValueEncoder,
    DayTimeDurationTermValueEncoder, DecimalTermValueEncoder, DoubleTermValueEncoder,
    FloatTermValueEncoder, IntTermValueEncoder, IntegerTermValueEncoder,
    NamedNodeRefTermValueEncoder, NamedNodeTermValueEncoder, NumericTypedValueEncoder,
    OwnedStringLiteralTermValueEncoder, SimpleLiteralRefTermValueEncoder,
};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_functions_scalar::{
    AbsSparqlOp, AsDecimalSparqlOp, AsDoubleSparqlOp, AsFloatSparqlOp, AsIntSparqlOp,
    AsIntegerSparqlOp, AsStringSparqlOp, BNodeSparqlOp, BoundSparqlOp, CeilSparqlOp,
    DatatypeSparqlOp, DaySparqlOp, EncodeForUriSparqlOp, FloorSparqlOp, HoursSparqlOp, IriSparqlOp,
    IsBlankSparqlOp, IsIriLegacySparqlOp, IsLiteralSparqlOp, IsNumericSparqlOp, LCaseSparqlOp,
    LangSparqlOp, Md5SparqlOp, MinutesSparqlOp, MonthSparqlOp, RoundSparqlOp, SecondsSparqlOp,
    Sha1SparqlOp, Sha256SparqlOp, Sha384SparqlOp, Sha512SparqlOp, StrLenSparqlOp, StrTypedValueOp,
    TimezoneSparqlOp, TzSparqlOp, UCaseSparqlOp, UnaryMinusSparqlOp, UnaryPlusSparqlOp,
    YearSparqlOp,
};
use rdf_fusion_functions_scalar::{AsBooleanSparqlOp, AsDateTimeSparqlOp};
use rdf_fusion_model::Iri;
use std::sync::Arc;

// Conversion
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    as_boolean_typed_value,
    AsBooleanSparqlOp,
    FunctionName::Builtin(BuiltinName::AsBoolean)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DateTimeTermValueEncoder,
    as_date_time_typed_value,
    AsDateTimeSparqlOp,
    FunctionName::Builtin(BuiltinName::CastDateTime)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DecimalTermValueEncoder,
    as_decimal_typed_value,
    AsDecimalSparqlOp,
    FunctionName::Builtin(BuiltinName::CastDecimal)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DoubleTermValueEncoder,
    as_double_typed_value,
    AsDoubleSparqlOp,
    FunctionName::Builtin(BuiltinName::CastDouble)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    FloatTermValueEncoder,
    as_float_typed_value,
    AsFloatSparqlOp,
    FunctionName::Builtin(BuiltinName::CastFloat)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    IntTermValueEncoder,
    as_int_typed_value,
    AsIntSparqlOp,
    FunctionName::Builtin(BuiltinName::AsInt)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    IntegerTermValueEncoder,
    as_integer_typed_value,
    AsIntegerSparqlOp,
    FunctionName::Builtin(BuiltinName::CastInteger)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    as_string_typed_value,
    AsStringSparqlOp,
    FunctionName::Builtin(BuiltinName::CastString)
);

// Dates and Times
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    day_typed_value,
    DaySparqlOp,
    FunctionName::Builtin(BuiltinName::Day)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    hours_typed_value,
    HoursSparqlOp,
    FunctionName::Builtin(BuiltinName::Hours)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    minutes_typed_value,
    MinutesSparqlOp,
    FunctionName::Builtin(BuiltinName::Minutes)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    month_typed_value,
    MonthSparqlOp,
    FunctionName::Builtin(BuiltinName::Month)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    DecimalTermValueEncoder,
    seconds_typed_value,
    SecondsSparqlOp,
    FunctionName::Builtin(BuiltinName::Seconds)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    DayTimeDurationTermValueEncoder,
    timezone_typed_value,
    TimezoneSparqlOp,
    FunctionName::Builtin(BuiltinName::Timezone)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    tz_typed_value,
    TzSparqlOp,
    FunctionName::Builtin(BuiltinName::Tz)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    year_typed_value,
    YearSparqlOp,
    FunctionName::Builtin(BuiltinName::Year)
);

// Functional Form
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder, // For Bound
    bound_typed_value,
    BoundSparqlOp,
    FunctionName::Builtin(BuiltinName::Bound)
);

// Hashing
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    md5_typed_value,
    Md5SparqlOp,
    FunctionName::Builtin(BuiltinName::Md5)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    sha1_typed_value,
    Sha1SparqlOp,
    FunctionName::Builtin(BuiltinName::Sha1)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    sha256_typed_value,
    Sha256SparqlOp,
    FunctionName::Builtin(BuiltinName::Sha256)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    sha384_typed_value,
    Sha384SparqlOp,
    FunctionName::Builtin(BuiltinName::Sha384)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    sha512_typed_value,
    Sha512SparqlOp,
    FunctionName::Builtin(BuiltinName::Sha512)
);

// Numeric
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    abs_typed_value,
    AbsSparqlOp,
    FunctionName::Builtin(BuiltinName::Abs)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    ceil_typed_value,
    CeilSparqlOp,
    FunctionName::Builtin(BuiltinName::Ceil)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    floor_typed_value,
    FloorSparqlOp,
    FunctionName::Builtin(BuiltinName::Floor)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    round_typed_value,
    RoundSparqlOp,
    FunctionName::Builtin(BuiltinName::Round)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    unary_minus_typed_value,
    UnaryMinusSparqlOp,
    FunctionName::Builtin(BuiltinName::UnaryMinus)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    unary_plus_typed_value,
    UnaryPlusSparqlOp,
    FunctionName::Builtin(BuiltinName::UnaryPlus)
);

// Strings
impl_unary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    encode_for_uri_typed_value,
    EncodeForUriSparqlOp,
    FunctionName::Builtin(BuiltinName::EncodeForUri)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    lcase_typed_value,
    LCaseSparqlOp,
    FunctionName::Builtin(BuiltinName::LCase)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    IntegerTermValueEncoder,
    str_len_typed_value,
    StrLenSparqlOp,
    FunctionName::Builtin(BuiltinName::StrLen)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    ucase_typed_value,
    UCaseSparqlOp,
    FunctionName::Builtin(BuiltinName::UCase)
);

// Terms
impl_unary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    BlankNodeRefTermValueEncoder,
    bnode_unary_typed_value,
    BNodeSparqlOp,
    FunctionName::Builtin(BuiltinName::BNode)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    NamedNodeRefTermValueEncoder,
    datatype_typed_value,
    DatatypeSparqlOp,
    FunctionName::Builtin(BuiltinName::Datatype)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    is_blank_typed_value,
    IsBlankSparqlOp,
    FunctionName::Builtin(BuiltinName::IsBlank)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    is_iri_typed_value,
    IsIriLegacySparqlOp,
    FunctionName::Builtin(BuiltinName::IsIri)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    is_literal_typed_value,
    IsLiteralSparqlOp,
    FunctionName::Builtin(BuiltinName::IsLiteral)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    is_numeric_typed_value,
    IsNumericSparqlOp,
    FunctionName::Builtin(BuiltinName::IsNumeric)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    SimpleLiteralRefTermValueEncoder,
    lang_typed_value,
    LangSparqlOp,
    FunctionName::Builtin(BuiltinName::Lang)
);
impl_unary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    str_typed_value,
    StrTypedValueOp,
    FunctionName::Builtin(BuiltinName::Str)
);

pub fn iri_typed_value(base_iri: Option<Iri<String>>) -> Arc<ScalarUDF> {
    let op = IriSparqlOp::new(base_iri);
    let udf_impl = UnaryScalarUdfOp::<
        IriSparqlOp,
        TypedValueEncoding,
        DefaultTypedValueDecoder,
        NamedNodeTermValueEncoder,
    >::new(&FunctionName::Builtin(BuiltinName::Iri), op);
    Arc::new(ScalarUDF::new_from_impl(udf_impl))
}
