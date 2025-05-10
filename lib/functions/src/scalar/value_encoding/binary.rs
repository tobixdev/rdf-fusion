use graphfusion_encoding::value_encoding::decoders::{
    DefaultTypedValueDecoder, IntegerTermValueDecoder, NamedNodeRefTermValueDecoder,
    NumericTermValueDecoder, SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use graphfusion_encoding::value_encoding::encoders::{
    BooleanTermValueEncoder, LiteralRefTermValueEncoder, NumericTermValueEncoder,
    OwnedStringLiteralTermValueEncoder, StringLiteralRefTermValueEncoder,
};
use graphfusion_encoding::value_encoding::TypedValueEncoding;
use graphfusion_encoding::TermEncoding;
use graphfusion_functions_scalar::{
    AddSparqlOp, ContainsSparqlOp, DivSparqlOp, EqSparqlOp, GreaterOrEqualSparqlOp,
    GreaterThanSparqlOp, LangMatchesSparqlOp, LessOrEqualSparqlOp, LessThanSparqlOp, MulSparqlOp,
    RegexSparqlOp, SameTermSparqlOp, StrAfterSparqlOp, StrBeforeSparqlOp, StrDtSparqlOp,
    StrEndsSparqlOp, StrLangSparqlOp, StrStartsSparqlOp, SubSparqlOp, SubStrSparqlOp,
};
use crate::builtin::BuiltinName;

// Comparisons
impl_binary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    SameTermValueBinaryDispatcher,
    SameTermSparqlOp,
    BuiltinName::SameTerm
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    EqValueBinaryDispatcher,
    EqSparqlOp,
    BuiltinName::Eq
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    GreaterThanValueBinaryDispatcher,
    GreaterThanSparqlOp,
    BuiltinName::GreaterThan
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    GreaterOrEqualValueBinaryDispatcher,
    GreaterOrEqualSparqlOp,
    BuiltinName::GreaterOrEqual
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    LessThanValueBinaryDispatcher,
    LessThanSparqlOp,
    BuiltinName::LessThan
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    LessOrEqualValueBinaryDispatcher,
    LessOrEqualSparqlOp,
    BuiltinName::LessOrEqual
);

// Numeric
impl_binary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    AddValueBinaryDispatcher,
    AddSparqlOp,
    BuiltinName::Add
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    DivValueBinaryDispatcher,
    DivSparqlOp,
    BuiltinName::Div
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    MulValueBinaryDispatcher,
    MulSparqlOp,
    BuiltinName::Mul
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    SubValueBinaryDispatcher,
    SubSparqlOp,
    BuiltinName::Sub
);

// Strings
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    ContainsValueBinaryDispatcher,
    ContainsSparqlOp,
    BuiltinName::Contains
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    LangMatchesValueBinaryDispatcher,
    LangMatchesSparqlOp,
    BuiltinName::LangMatches
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    RegexValueBinaryDispatcher,
    RegexSparqlOp,
    BuiltinName::Regex
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueEncoder,
    StrAfterValueBinaryDispatcher,
    StrAfterSparqlOp,
    BuiltinName::StrAfter
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueEncoder,
    StrBeforeValueBinaryDispatcher,
    StrBeforeSparqlOp,
    BuiltinName::StrBefore
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    StrEndsValueBinaryDispatcher,
    StrEndsSparqlOp,
    BuiltinName::StrEnds
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    StrStartsValueBinaryDispatcher,
    StrStartsSparqlOp,
    BuiltinName::StrStarts
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    IntegerTermValueDecoder,
    StringLiteralRefTermValueEncoder,
    SubStrValueBinaryDispatcher,
    SubStrSparqlOp,
    BuiltinName::SubStr
);

// Terms
impl_binary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    NamedNodeRefTermValueDecoder,
    LiteralRefTermValueEncoder,
    SubDtValueBinaryDispatcher,
    StrDtSparqlOp,
    BuiltinName::StrDt
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    SubLangValueBinaryDispatcher,
    StrLangSparqlOp,
    BuiltinName::StrLang
);
