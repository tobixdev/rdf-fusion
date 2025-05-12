use crate::builtin::BuiltinName;
use graphfusion_encoding::typed_value::decoders::{
    DefaultTypedValueDecoder, IntegerTermValueDecoder, NamedNodeRefTermValueDecoder,
    NumericTermValueDecoder, SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use graphfusion_encoding::typed_value::encoders::{
    BooleanTermValueEncoder, LiteralRefTermValueEncoder, NumericTypedValueEncoder,
    OwnedStringLiteralTermValueEncoder, StringLiteralRefTermValueEncoder,
};
use graphfusion_encoding::typed_value::TypedValueEncoding;
use graphfusion_encoding::TermEncoding;
use graphfusion_functions_scalar::{
    AddSparqlOp, ContainsSparqlOp, DivSparqlOp, EqSparqlOp, GreaterOrEqualSparqlOp,
    GreaterThanSparqlOp, LangMatchesSparqlOp, LessOrEqualSparqlOp, LessThanSparqlOp, MulSparqlOp,
    RegexSparqlOp, SameTermSparqlOp, StrAfterSparqlOp, StrBeforeSparqlOp, StrDtSparqlOp,
    StrEndsSparqlOp, StrLangSparqlOp, StrStartsSparqlOp, SubSparqlOp, SubStrSparqlOp,
};

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
    BuiltinName::Equal
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
    NumericTypedValueEncoder,
    AddTypedValueFactory,
    AddSparqlOp,
    BuiltinName::Add
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    DivTypedValueFactory,
    DivSparqlOp,
    BuiltinName::Div
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
    MulTypedValueFactory,
    MulSparqlOp,
    BuiltinName::Mul
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTypedValueEncoder,
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
    RegexBinaryTypedValueFactory,
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
    StrDtTypedValueFactory,
    StrDtSparqlOp,
    BuiltinName::StrDt
);
impl_binary_sparql_op!(
    TypedValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    StrLangTypedValueFactory,
    StrLangSparqlOp,
    BuiltinName::StrLang
);
