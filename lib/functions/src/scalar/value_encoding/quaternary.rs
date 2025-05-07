use crate::dispatcher::SparqlOpDispatcher;
use crate::scalar::quaternary::dispatch_quaternary;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr_common::signature::Signature;
use graphfusion_encoding::value_encoding::decoders::{
    SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use graphfusion_encoding::value_encoding::encoders::OwnedStringLiteralTermValueEncoder;
use graphfusion_encoding::value_encoding::TermValueEncoding;
use graphfusion_encoding::TermEncoding;
use graphfusion_functions_scalar::ReplaceSparqlOp;
use graphfusion_functions_scalar::SparqlOp;

// Strings
impl_quarternary_rdf_value_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    RegexTermValueQuarternaryDispatcher,
    ReplaceSparqlOp
);
