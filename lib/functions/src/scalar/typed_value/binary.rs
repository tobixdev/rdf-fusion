use crate::builtin::BuiltinName;
use crate::FunctionName;
use rdf_fusion_encoding::typed_value::decoders::{
    DefaultTypedValueDecoder, IntegerTermValueDecoder, NamedNodeRefTermValueDecoder,
    NumericTermValueDecoder, SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use rdf_fusion_encoding::typed_value::encoders::{
    BooleanTermValueEncoder, LiteralRefTermValueEncoder, NumericTypedValueEncoder,
    OwnedStringLiteralTermValueEncoder, StringLiteralRefTermValueEncoder,
};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_functions_scalar::{
    AddSparqlOp, ContainsSparqlOp, DivSparqlOp, EqTypedValueSparqlOp, GreaterOrEqualSparqlOp,
    GreaterThanSparqlOp, LangMatchesSparqlOp, LessOrEqualSparqlOp, LessThanSparqlOp, MulSparqlOp,
    RegexSparqlOp, StrAfterSparqlOp, StrBeforeSparqlOp, StrDtSparqlOp, StrEndsSparqlOp,
    StrLangSparqlOp, StrStartsSparqlOp, SubSparqlOp, SubStrSparqlOp,
};

// Comparisons
impl_binary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    equal_typed_value,
    EqTypedValueSparqlOp,
    FunctionName::Builtin(BuiltinName::Equal)
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    greater_than_typed_value,
    GreaterThanSparqlOp,
    FunctionName::Builtin(BuiltinName::GreaterThan)
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    greater_or_equal_typed_value,
    GreaterOrEqualSparqlOp,
    FunctionName::Builtin(BuiltinName::GreaterOrEqual)
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    less_than_typed_value,
    LessThanSparqlOp,
    FunctionName::Builtin(BuiltinName::LessThan)
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    less_or_equal_typed_value,
    LessOrEqualSparqlOp,
    FunctionName::Builtin(BuiltinName::LessOrEqual)
);

// Numeric
impl_binary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    add_typed_value,
    AddSparqlOp,
    FunctionName::Builtin(BuiltinName::Add)
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    div_typed_value,
    DivSparqlOp,
    FunctionName::Builtin(BuiltinName::Div)
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    mul_typed_value,
    MulSparqlOp,
    FunctionName::Builtin(BuiltinName::Mul)
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    sub_typed_value,
    SubSparqlOp,
    FunctionName::Builtin(BuiltinName::Sub)
);

// Strings
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    contains_typed_value,
    ContainsSparqlOp,
    FunctionName::Builtin(BuiltinName::Contains)
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    lang_matches_typed_value,
    LangMatchesSparqlOp,
    FunctionName::Builtin(BuiltinName::LangMatches)
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    regex_binary_typed_value,
    RegexSparqlOp,
    FunctionName::Builtin(BuiltinName::Regex)
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueEncoder,
    str_after_typed_value,
    StrAfterSparqlOp,
    FunctionName::Builtin(BuiltinName::StrAfter)
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueEncoder,
    str_before_typed_value,
    StrBeforeSparqlOp,
    FunctionName::Builtin(BuiltinName::StrBefore)
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    str_ends_typed_value,
    StrEndsSparqlOp,
    FunctionName::Builtin(BuiltinName::StrEnds)
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    str_starts_typed_value,
    StrStartsSparqlOp,
    FunctionName::Builtin(BuiltinName::StrStarts)
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    IntegerTermValueDecoder,
    StringLiteralRefTermValueEncoder,
    sub_str_binary_typed_value,
    SubStrSparqlOp,
    FunctionName::Builtin(BuiltinName::SubStr)
);

// Terms
impl_binary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    NamedNodeRefTermValueDecoder,
    LiteralRefTermValueEncoder,
    str_dt_typed_value,
    StrDtSparqlOp,
    FunctionName::Builtin(BuiltinName::StrDt)
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    str_lang_typed_value,
    StrLangSparqlOp,
    FunctionName::Builtin(BuiltinName::StrLang)
);
