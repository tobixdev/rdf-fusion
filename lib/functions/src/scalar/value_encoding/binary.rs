use crate::dispatcher::SparqlOpDispatcher;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr_common::signature::Signature;
use datafusion::physical_plan::ColumnarValue;
use graphfusion_encoding::value_encoding::decoders::{
    DefaultTermValueDecoder, IntegerTermValueDecoder, NamedNodeRefTermValueDecoder,
    NumericTermValueDecoder, SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use graphfusion_encoding::value_encoding::encoders::{
    BooleanTermValueEncoder, LiteralRefTermValueEncoder, NumericTermValueEncoder,
    OwnedStringLiteralTermValueEncoder, StringLiteralRefTermValueEncoder,
};
use graphfusion_encoding::value_encoding::TermValueEncoding;
use graphfusion_encoding::TermEncoding;
use graphfusion_functions_scalar::SparqlOp;
use graphfusion_functions_scalar::{
    AddSparqlOp, ContainsSparqlOp, DivSparqlOp, EqSparqlOp, GreaterOrEqualSparqlOp,
    GreaterThanSparqlOp, LangMatchesSparqlOp, LessOrEqualSparqlOp, LessThanSparqlOp, MulSparqlOp,
    RegexSparqlOp, SameTermSparqlOp, StrAfterSparqlOp, StrBeforeSparqlOp, StrDtSparqlOp,
    StrEndsSparqlOp, StrLangSparqlOp, StrStartsSparqlOp, SubSparqlOp, SubStrSparqlOp,
};

// Comparisons
impl_binary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    DefaultTermValueDecoder,
    BooleanTermValueEncoder,
    SameTermValueBinaryDispatcher,
    SameTermSparqlOp
);
impl_binary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    DefaultTermValueDecoder,
    BooleanTermValueEncoder,
    EqValueBinaryDispatcher,
    EqSparqlOp
);
impl_binary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    DefaultTermValueDecoder,
    BooleanTermValueEncoder,
    GreaterThanValueBinaryDispatcher,
    GreaterThanSparqlOp
);
impl_binary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    DefaultTermValueDecoder,
    BooleanTermValueEncoder,
    GreaterOrEqualValueBinaryDispatcher,
    GreaterOrEqualSparqlOp
);
impl_binary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    DefaultTermValueDecoder,
    BooleanTermValueEncoder,
    LessThanValueBinaryDispatcher,
    LessThanSparqlOp
);
impl_binary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    DefaultTermValueDecoder,
    BooleanTermValueEncoder,
    LessOrEqualValueBinaryDispatcher,
    LessOrEqualSparqlOp
);

// Numeric
impl_binary_sparql_op!(
    TermValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    AddValueBinaryDispatcher,
    AddSparqlOp
);
impl_binary_sparql_op!(
    TermValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    DivValueBinaryDispatcher,
    DivSparqlOp
);
impl_binary_sparql_op!(
    TermValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    MulValueBinaryDispatcher,
    MulSparqlOp
);
impl_binary_sparql_op!(
    TermValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    SubValueBinaryDispatcher,
    SubSparqlOp
);

// Strings
impl_binary_sparql_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    ContainsValueBinaryDispatcher,
    ContainsSparqlOp
);
impl_binary_sparql_op!(
    TermValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    LangMatchesValueBinaryDispatcher,
    LangMatchesSparqlOp
);
impl_binary_sparql_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    RegexValueBinaryDispatcher,
    RegexSparqlOp
);
impl_binary_sparql_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueEncoder,
    StrAfterValueBinaryDispatcher,
    StrAfterSparqlOp
);
impl_binary_sparql_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueEncoder,
    StrBeforeValueBinaryDispatcher,
    StrBeforeSparqlOp
);
impl_binary_sparql_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    StrEndsValueBinaryDispatcher,
    StrEndsSparqlOp
);
impl_binary_sparql_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    StringLiteralRefTermValueDecoder,
    BooleanTermValueEncoder,
    StrStartsValueBinaryDispatcher,
    StrStartsSparqlOp
);
impl_binary_sparql_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    IntegerTermValueDecoder,
    StringLiteralRefTermValueEncoder,
    SubStrValueBinaryDispatcher,
    SubStrSparqlOp
);

// Terms
impl_binary_sparql_op!(
    TermValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    NamedNodeRefTermValueDecoder,
    LiteralRefTermValueEncoder,
    SubDtValueBinaryDispatcher,
    StrDtSparqlOp
);
impl_binary_sparql_op!(
    TermValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    SubLangValueBinaryDispatcher,
    StrLangSparqlOp
);
