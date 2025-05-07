use crate::dispatcher::SparqlOpDispatcher;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr_common::signature::Signature;
use graphfusion_encoding::value_encoding::decoders::{
    BooleanTermValueDecoder, DefaultTermValueDecoder, IntegerTermValueDecoder,
    SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use graphfusion_encoding::value_encoding::encoders::{
    BooleanTermValueEncoder, DefaultTermValueEncoder, OwnedStringLiteralTermValueEncoder,
    StringLiteralRefTermValueEncoder,
};
use graphfusion_encoding::value_encoding::TermValueEncoding;
use graphfusion_encoding::{EncodingArray, TermDecoder, TermEncoder, TermEncoding};
use graphfusion_functions_scalar::SparqlOp;
use graphfusion_functions_scalar::{
    IfSparqlOp, RegexSparqlOp, ReplaceSparqlOp, SubStrSparqlOp, TernaryRdfTermValueOp,
};
use crate::scalar::ternary::dispatch_ternary;

// Functional Forms
impl_ternary_rdf_value_op!(
    TermValueEncoding,
    BooleanTermValueDecoder,
    DefaultTermValueDecoder,
    DefaultTermValueDecoder,
    DefaultTermValueEncoder,
    IfValueTernaryDispatcher,
    IfSparqlOp
);

// Strings
impl_ternary_rdf_value_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    RegexValueTernaryDispatcher,
    RegexSparqlOp
);
impl_ternary_rdf_value_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    ReplaceValueTernaryDispatcher,
    ReplaceSparqlOp
);
impl_ternary_rdf_value_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    IntegerTermValueDecoder,
    IntegerTermValueDecoder,
    StringLiteralRefTermValueEncoder,
    SubStrTernaryDispatcher,
    SubStrSparqlOp
);
