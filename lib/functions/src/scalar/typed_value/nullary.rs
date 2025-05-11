use crate::builtin::BuiltinName;
use graphfusion_encoding::typed_value::encoders::{
    BlankNodeTermValueEncoder, DoubleTermValueEncoder, NamedNodeTermValueEncoder,
    OwnedStringLiteralTermValueEncoder,
};
use graphfusion_encoding::typed_value::TypedValueEncoding;
use graphfusion_encoding::TermEncoding;
use graphfusion_functions_scalar::{BNodeSparqlOp, RandSparqlOp, StrUuidSparqlOp, UuidSparqlOp};

impl_nullary_op!(
    TypedValueEncoding,
    BlankNodeTermValueEncoder,
    BNodeNullaryTypedValueFactory,
    BNodeSparqlOp,
    BuiltinName::BNode
);
impl_nullary_op!(
    TypedValueEncoding,
    DoubleTermValueEncoder,
    RandTypedValueFactory,
    RandSparqlOp,
    BuiltinName::Rand
);
impl_nullary_op!(
    TypedValueEncoding,
    OwnedStringLiteralTermValueEncoder,
    StrUuidTypedValueFactory,
    StrUuidSparqlOp,
    BuiltinName::StrUuid
);
impl_nullary_op!(
    TypedValueEncoding,
    NamedNodeTermValueEncoder,
    UuidTypedValueFactory,
    UuidSparqlOp,
    BuiltinName::Uuid
);
