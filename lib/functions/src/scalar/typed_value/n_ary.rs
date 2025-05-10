use crate::impl_n_ary_sparql_op;
use graphfusion_encoding::value_encoding::decoders::{
    DefaultTypedValueDecoder, StringLiteralRefTermValueDecoder,
};
use graphfusion_encoding::value_encoding::encoders::{
    DefaultTermValueEncoder, OwnedStringLiteralTermValueEncoder,
};
use graphfusion_encoding::value_encoding::TypedValueEncoding;
use graphfusion_functions_scalar::{CoalesceSparqlOp, ConcatSparqlOp};
use crate::builtin::BuiltinName;

// Functional Forms
impl_n_ary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTermValueEncoder,
    CoalesceTypedValueFactory,
    CoalesceSparqlOp,
    BuiltinName::Coalesce
);

// Strings
impl_n_ary_sparql_op!(
    TypedValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    ConcatTypedValueFactory,
    ConcatSparqlOp,
    BuiltinName::Concat
);
