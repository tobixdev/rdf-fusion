use crate::builtin::BuiltinName;
use rdf_fusion_encoding::typed_value::decoders::{
    SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use rdf_fusion_encoding::typed_value::encoders::OwnedStringLiteralTermValueEncoder;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_functions_scalar::ReplaceSparqlOp;
use crate::FunctionName;

// Strings
impl_quarternary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    regex_term_typed_value,
    ReplaceSparqlOp,
    FunctionName::Builtin(BuiltinName::Replace)
);
