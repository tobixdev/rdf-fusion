use crate::builtin::BuiltinName;
use graphfusion_encoding::typed_value::decoders::{
    BooleanTermValueDecoder, DefaultTypedValueDecoder, IntegerTermValueDecoder,
    SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use graphfusion_encoding::typed_value::encoders::{
    BooleanTermValueEncoder, DefaultTypedValueEncoder, OwnedStringLiteralTermValueEncoder,
    StringLiteralRefTermValueEncoder,
};
use graphfusion_encoding::typed_value::TypedValueEncoding;
use graphfusion_encoding::TermEncoding;
use graphfusion_functions_scalar::{IfSparqlOp, RegexSparqlOp, ReplaceSparqlOp, SubStrSparqlOp};

// Functional Forms
impl_ternary_sparql_op!(
    TypedValueEncoding,
    BooleanTermValueDecoder,
    DefaultTypedValueDecoder,
    DefaultTypedValueDecoder,
    DefaultTypedValueEncoder,
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
