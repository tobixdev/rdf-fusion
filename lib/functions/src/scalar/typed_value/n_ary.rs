use crate::builtin::BuiltinName;
use crate::{impl_n_ary_sparql_op, FunctionName};
use rdf_fusion_encoding::typed_value::decoders::{
    DefaultTypedValueDecoder, StringLiteralRefTermValueDecoder,
};
use rdf_fusion_encoding::typed_value::encoders::{
    DefaultTypedValueEncoder, OwnedStringLiteralTermValueEncoder,
};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_functions_scalar::{CoalesceSparqlOp, ConcatSparqlOp};

// Functional Forms
impl_n_ary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueEncoder,
    coalesce_typed_value,
    CoalesceSparqlOp,
    FunctionName::Builtin(BuiltinName::Coalesce)
);

// Strings
impl_n_ary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    concat_typed_value,
    ConcatSparqlOp,
    FunctionName::Builtin(BuiltinName::Concat)
);
