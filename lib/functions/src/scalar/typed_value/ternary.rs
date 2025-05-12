use crate::builtin::BuiltinName;
use crate::FunctionName;
use graphfusion_encoding::typed_value::decoders::{
    BooleanTermValueDecoder, DefaultTypedValueDecoder, IntegerTermValueDecoder,
    SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use graphfusion_encoding::typed_value::encoders::{
    BooleanTermValueEncoder, DefaultTypedValueEncoder, OwnedStringLiteralTermValueEncoder,
    StringLiteralRefTermValueEncoder,
};
use graphfusion_encoding::typed_value::TypedValueEncoding;
use graphfusion_functions_scalar::{IfSparqlOp, RegexSparqlOp, ReplaceSparqlOp, SubStrSparqlOp};

// Functional Forms
impl_ternary_sparql_op!(
    TypedValueEncoding,
    BooleanTermValueDecoder,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    DefaultTypedValueEncoder,
    if_typed_value,
    IfSparqlOp,
    FunctionName::Builtin(BuiltinName::If)
);

// Strings
impl_ternary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    regex_ternary_typed_value,
    RegexSparqlOp,
    FunctionName::Builtin(BuiltinName::Regex)
);
impl_ternary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    replace_typed_value,
    ReplaceSparqlOp,
    FunctionName::Builtin(BuiltinName::Replace)
);
impl_ternary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    IntegerTermValueDecoder,
    IntegerTermValueDecoder,
    StringLiteralRefTermValueEncoder,
    sub_str_ternary_typed_value,
    SubStrSparqlOp,
    FunctionName::Builtin(BuiltinName::SubStr)
);
