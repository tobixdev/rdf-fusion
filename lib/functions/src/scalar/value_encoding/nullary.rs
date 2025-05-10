use crate::builtin::BuiltinName;
use graphfusion_encoding::value_encoding::encoders::{
    BlankNodeTermValueEncoder, DoubleTermValueEncoder, NamedNodeTermValueEncoder,
    OwnedStringLiteralTermValueEncoder,
};
use graphfusion_encoding::value_encoding::TypedValueEncoding;
use graphfusion_encoding::TermEncoding;
use graphfusion_functions_scalar::{BNodeSparqlOp, RandSparqlOp, StrUuidSparqlOp, UuidSparqlOp};

impl_nullary_op!(
    TypedValueEncoding,
    BlankNodeTermValueEncoder,
    BNodeTermValue,
    BNodeSparqlOp,
    BuiltinName::BNode
);
impl_nullary_op!(
    TypedValueEncoding,
    DoubleTermValueEncoder,
    RandTermValue,
    RandSparqlOp,
    BuiltinName::Rand
);
impl_nullary_op!(
    TypedValueEncoding,
    OwnedStringLiteralTermValueEncoder,
    StrUuidTermValue,
    StrUuidSparqlOp,
    BuiltinName::StrUuid
);
impl_nullary_op!(
    TypedValueEncoding,
    NamedNodeTermValueEncoder,
    UuidTermValue,
    UuidSparqlOp,
    BuiltinName::Uuid
);
