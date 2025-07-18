use crate::builtin::BuiltinName;
use crate::FunctionName;
use rdf_fusion_encoding::typed_value::decoders::{
    BooleanTermValueDecoder, DefaultTypedValueDecoder, SimpleLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
};
use rdf_fusion_encoding::typed_value::encoders::{
    DefaultTypedValueEncoder, OwnedStringLiteralTermValueEncoder,
};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_functions_scalar::{IfSparqlOp, ReplaceSparqlOp};

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
    OwnedStringLiteralTermValueEncoder,
    replace_typed_value,
    ReplaceSparqlOp,
    FunctionName::Builtin(BuiltinName::Replace)
);
