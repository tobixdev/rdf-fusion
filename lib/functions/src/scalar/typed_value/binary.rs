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
    SameTermTypedValueFactory,
    SameTermSparqlOp,
    BuiltinName::SameTerm
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    EqTypedValueFactory,
    EqSparqlOp,
    BuiltinName::Eq
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    GreaterThanTypedValueFactory,
    GreaterThanSparqlOp,
    BuiltinName::GreaterThan
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    GreaterOrEqualTypedValueFactory,
    GreaterOrEqualSparqlOp,
    BuiltinName::GreaterOrEqual
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    LessThanTypedValueFactory,
    LessThanSparqlOp,
    BuiltinName::LessThan
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    BooleanTermValueEncoder,
    LessOrEqualTypedValueFactory,
    LessOrEqualSparqlOp,
    BuiltinName::LessOrEqual
);

// Numeric
impl_binary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    AddTypedValueFactory,
    AddSparqlOp,
    BuiltinName::Add
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    DivTypedValueFactory,
    DivSparqlOp,
    BuiltinName::Div
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    MulTypedValueFactory,
    MulSparqlOp,
    BuiltinName::Mul
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    SubTypedValueFactory,
    SubSparqlOp,
    BuiltinName::Sub
);

// Strings
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    ContainsTypedValueFactory,
    ContainsSparqlOp,
    BuiltinName::Contains
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    LangMatchesTypedValueFactory,
    LangMatchesSparqlOp,
    BuiltinName::LangMatches
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    RegexTypedValueFactory,
    RegexSparqlOp,
    BuiltinName::Regex
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueEncoder,
    StrAfterTypedValueFactory,
    StrAfterSparqlOp,
    BuiltinName::StrAfter
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueEncoder,
    StrBeforeTypedValueFactory,
    StrBeforeSparqlOp,
    BuiltinName::StrBefore
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    StrEndsTypedValueFactory,
    StrEndsSparqlOp,
    BuiltinName::StrEnds
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    StrStartsTypedValueFactory,
    StrStartsSparqlOp,
    BuiltinName::StrStarts
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    IntegerTermValueDecoder,
    StringLiteralRefTermValueEncoder,
    SubStrTypedValueFactory,
    SubStrSparqlOp,
    BuiltinName::SubStr
);

// Terms
impl_binary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    NamedNodeRefTermValueDecoder,
    LiteralRefTermValueEncoder,
    SubDtTypedValueFactory,
    StrDtSparqlOp,
    BuiltinName::StrDt
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    SubLangTypedValueFactory,
    StrLangSparqlOp,
    BuiltinName::StrLang
);
