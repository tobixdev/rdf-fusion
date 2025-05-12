use crate::builtin::BuiltinName;
use crate::FunctionName;
use graphfusion_encoding::typed_value::encoders::{
    BlankNodeTermValueEncoder, DoubleTermValueEncoder, NamedNodeTermValueEncoder,
    OwnedStringLiteralTermValueEncoder,
};
use graphfusion_encoding::typed_value::TypedValueEncoding;
use graphfusion_functions_scalar::{BNodeSparqlOp, RandSparqlOp, StrUuidSparqlOp, UuidSparqlOp};

impl_nullary_op!(
    TypedValueEncoding,
    BlankNodeTermValueEncoder,
    bnode_nullary_typed_value,
    BNodeSparqlOp,
    FunctionName::Builtin(BuiltinName::BNode)
);
impl_nullary_op!(
    TypedValueEncoding,
    DoubleTermValueEncoder,
    rand_typed_value,
    RandSparqlOp,
    FunctionName::Builtin(BuiltinName::Rand)
);
impl_nullary_op!(
    TypedValueEncoding,
    OwnedStringLiteralTermValueEncoder,
    str_uuid_typed_value,
    StrUuidSparqlOp,
    FunctionName::Builtin(BuiltinName::StrUuid)
);
impl_nullary_op!(
    TypedValueEncoding,
    NamedNodeTermValueEncoder,
    uuid_typed_value,
    UuidSparqlOp,
    FunctionName::Builtin(BuiltinName::Uuid)
);
