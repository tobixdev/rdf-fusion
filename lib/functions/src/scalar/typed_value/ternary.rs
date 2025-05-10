use crate::builtin::BuiltinName;
use graphfusion_encoding::value_encoding::decoders::{
    BooleanTermValueDecoder, DefaultTypedValueDecoder, IntegerTermValueDecoder,
    SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use graphfusion_encoding::value_encoding::encoders::{
    BooleanTermValueEncoder, DefaultTermValueEncoder, OwnedStringLiteralTermValueEncoder,
    StringLiteralRefTermValueEncoder,
};
use graphfusion_encoding::value_encoding::TypedValueEncoding;
use graphfusion_encoding::TermEncoding;
use graphfusion_functions_scalar::{IfSparqlOp, RegexSparqlOp, ReplaceSparqlOp, SubStrSparqlOp};

// Functional Forms
impl_ternary_sparql_op!(
    TypedValueEncoding,
    BooleanTermValueDecoder,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    DefaultTermValueEncoder,
    IfTypedValueFactory,
    IfSparqlOp,
    BuiltinName::If
);

// Strings
impl_ternary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    RegexTypedValueFactory,
    RegexSparqlOp,
    BuiltinName::Regex
);
impl_ternary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    ReplaceTypedValueFactory,
    ReplaceSparqlOp,
    BuiltinName::Replace
);
impl_ternary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    IntegerTermValueDecoder,
    IntegerTermValueDecoder,
    StringLiteralRefTermValueEncoder,
    SubStrTernaryDispatcher,
    SubStrSparqlOp,
    BuiltinName::SubStr
);
