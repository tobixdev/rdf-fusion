use graphfusion_encoding::value_encoding::decoders::{
    SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use graphfusion_encoding::value_encoding::encoders::OwnedStringLiteralTermValueEncoder;
use graphfusion_encoding::value_encoding::TypedValueEncoding;
use graphfusion_encoding::TermEncoding;
use graphfusion_functions_scalar::ReplaceSparqlOp;
use crate::builtin::BuiltinName;

// Strings
impl_quarternary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    RegexTermValueQuarternaryDispatcher,
    ReplaceSparqlOp,
    BuiltinName::Replace
);
