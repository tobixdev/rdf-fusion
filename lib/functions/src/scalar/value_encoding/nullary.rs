use crate::dispatcher::SparqlOpDispatcher;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, Signature};
use graphfusion_encoding::value_encoding::encoders::{
    BlankNodeTermValueEncoder, DoubleTermValueEncoder, NamedNodeTermValueEncoder,
    OwnedStringLiteralTermValueEncoder,
};
use graphfusion_encoding::value_encoding::TermValueEncoding;
use graphfusion_encoding::{EncodingArray, TermEncoder, TermEncoding};
use graphfusion_functions_scalar::{
    BNodeSparqlOp, NullarySparqlOp, RandSparqlOp, SparqlOp, StrUuidSparqlOp, UuidSparqlOp,
};
use std::fmt::Debug;

impl_nullary_op!(
    TermValueEncoding,
    BlankNodeTermValueEncoder,
    BNodeTermValue,
    BNodeSparqlOp
);
impl_nullary_op!(
    TermValueEncoding,
    DoubleTermValueEncoder,
    RandTermValue,
    RandSparqlOp
);
impl_nullary_op!(
    TermValueEncoding,
    OwnedStringLiteralTermValueEncoder,
    StrUuidTermValue,
    StrUuidSparqlOp
);
impl_nullary_op!(
    TermValueEncoding,
    NamedNodeTermValueEncoder,
    UuidTermValue,
    UuidSparqlOp
);
