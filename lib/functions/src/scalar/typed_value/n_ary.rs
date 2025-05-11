use crate::builtin::BuiltinName;
use crate::impl_n_ary_sparql_op;
use graphfusion_encoding::typed_value::decoders::{
    DefaultTypedValueDecoder, StringLiteralRefTermValueDecoder,
};
use graphfusion_encoding::typed_value::encoders::{
    DefaultTypedValueEncoder, OwnedStringLiteralTermValueEncoder,
};
use graphfusion_encoding::typed_value::TypedValueEncoding;
use graphfusion_functions_scalar::{CoalesceSparqlOp, ConcatSparqlOp};

// Functional Forms
impl_n_ary_sparql_op!(
    TypedValueEncoding,
    DefaultTypedValueDecoder,
    DefaultTypedValueEncoder,
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
