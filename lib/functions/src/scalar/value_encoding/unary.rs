use crate::dispatcher::SparqlOpDispatcher;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr_common::signature::Signature;
use graphfusion_encoding::value_encoding::decoders::{
    DateTimeTermValueDecoder, DefaultTermValueDecoder, NumericTermValueDecoder,
    SimpleLiteralRefTermValueDecoder, StringLiteralRefTermValueDecoder,
};
use graphfusion_encoding::value_encoding::encoders::{
    BlankNodeRefTermValueEncoder, BooleanTermValueEncoder, DateTimeTermValueEncoder,
    DayTimeDurationTermValueEncoder, DecimalTermValueEncoder,
    DoubleTermValueEncoder, FloatTermValueEncoder, IntTermValueEncoder, IntegerTermValueEncoder,
    NamedNodeRefTermValueEncoder, NamedNodeTermValueEncoder, NumericTermValueEncoder,
    OwnedStringLiteralTermValueEncoder, SimpleLiteralRefTermValueEncoder,
};
use graphfusion_encoding::value_encoding::TermValueEncoding;
use graphfusion_encoding::{EncodingArray, EncodingScalar, TermDecoder, TermEncoder, TermEncoding};
use graphfusion_functions_scalar::{
    AbsSparqlOp, AsDecimalSparqlOp, AsDoubleSparqlOp, AsFloatSparqlOp, AsIntSparqlOp,
    AsIntegerSparqlOp, AsStringSparqlOp, BNodeSparqlOp, BinaryTermValueOp, BoundSparqlOp,
    CeilSparqlOp, DatatypeSparqlOp, DaySparqlOp, EncodeForUriSparqlOp, FloorSparqlOp,
    HoursSparqlOp, IriSparqlOp, IsBlankSparqlOp, IsIriSparqlOp, IsLiteralSparqlOp,
    IsNumericSparqlOp, LCaseSparqlOp, LangSparqlOp, Md5SparqlOp, MinutesSparqlOp, MonthSparqlOp,
    RoundSparqlOp, SecondsSparqlOp, Sha1SparqlOp, Sha256SparqlOp, Sha384SparqlOp, Sha512SparqlOp,
    SparqlOp, StrLenSparqlOp, StrSparqlOp, TimezoneSparqlOp, TzSparqlOp, UCaseSparqlOp,
    UnaryMinusSparqlOp, UnaryPlusSparqlOp, YearSparqlOp,
};
use graphfusion_functions_scalar::{AsBooleanSparqlOp, AsDateTimeSparqlOp, UnaryTermValueOp};
use graphfusion_model::ThinError;
use std::fmt::Debug;

macro_rules! impl_unary_sparql_op {
    ($ENCODING: ty, $DECODER: ty, $ENCODER: ty, $STRUCT_NAME: ident, $SPARQL_OP: ty) => {
        #[derive(Debug)]
        struct $STRUCT_NAME {
            signature: Signature,
            op: $SPARQL_OP,
        }

        impl SparqlOpDispatcher for $STRUCT_NAME {
            fn name(&self) -> &str {
                self.op.name()
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
                Ok(<$ENCODING>::data_type())
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
                match TryInto::<[_; 1]>::try_into(args.args) {
                    Ok([ColumnarValue::Array(arg)]) => {
                        dispatch_unary_array::<$ENCODING, $DECODER, $ENCODER, $SPARQL_OP>(
                            &self.op,
                            &<$ENCODING>::try_new_array(arg)?,
                        )
                    }
                    Ok([ColumnarValue::Scalar(arg)]) => {
                        dispatch_unary_scalar::<$ENCODING, $DECODER, $ENCODER, $SPARQL_OP>(
                            &self.op,
                            &<$ENCODING>::try_new_scalar(arg)?,
                        )
                    }
                    _ => Err(DataFusionError::Execution(String::from(
                        "Unexpected type combination.",
                    ))),
                }
            }
        }
    };
}

// Conversion
impl_unary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    BooleanTermValueEncoder,
    AsBooleanValueUnaryDispatcher,
    AsBooleanSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    DateTimeTermValueEncoder,
    AsDateTimeValueUnaryDispatcher,
    AsDateTimeSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    DecimalTermValueEncoder,
    AsDecimalValueUnaryDispatcher,
    AsDecimalSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    DoubleTermValueEncoder,
    AsDoubleValueUnaryDispatcher,
    AsDoubleSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    FloatTermValueEncoder,
    AsFloatValueUnaryDispatcher,
    AsFloatSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    IntTermValueEncoder,
    AsIntValueUnaryDispatcher,
    AsIntSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    IntegerTermValueEncoder,
    AsIntegerValueUnaryDispatcher,
    AsIntegerSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    AsStringValueUnaryDispatcher,
    AsStringSparqlOp
);

// Dates and Times
impl_unary_sparql_op!(
    TermValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    DayValueUnaryDispatcher,
    DaySparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    HoursValueUnaryDispatcher,
    HoursSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    MinutesValueUnaryDispatcher,
    MinutesSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    MonthValueUnaryDispatcher,
    MonthSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DateTimeTermValueDecoder,
    DecimalTermValueEncoder,
    SecondsValueUnaryDispatcher,
    SecondsSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DateTimeTermValueDecoder,
    DayTimeDurationTermValueEncoder,
    TimezoneValueUnaryDispatcher,
    TimezoneSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DateTimeTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    TzValueUnaryDispatcher,
    TzSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DateTimeTermValueDecoder,
    IntegerTermValueEncoder,
    YearValueUnaryDispatcher,
    YearSparqlOp
);

// Functional Form
impl_unary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    BooleanTermValueEncoder, // For Bound
    BoundValueUnaryDispatcher,
    BoundSparqlOp
);

// Hashing
impl_unary_sparql_op!(
    TermValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    Md5ValueUnaryDispatcher,
    Md5SparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    Sha1ValueUnaryDispatcher,
    Sha1SparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    Sha256ValueUnaryDispatcher,
    Sha256SparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    Sha384ValueUnaryDispatcher,
    Sha384SparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    Sha512ValueUnaryDispatcher,
    Sha512SparqlOp
);

// Numeric
impl_unary_sparql_op!(
    TermValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    AbsValueUnaryDispatcher,
    AbsSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    CeilValueUnaryDispatcher,
    CeilSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    FloorValueUnaryDispatcher,
    FloorSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    RoundValueUnaryDispatcher,
    RoundSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    UnaryMinusValueUnaryDispatcher,
    UnaryMinusSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    NumericTermValueDecoder,
    NumericTermValueEncoder,
    UnaryPlusValueUnaryDispatcher,
    UnaryPlusSparqlOp
);

// Strings
impl_unary_sparql_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    EncodeForUriValueUnaryDispatcher,
    EncodeForUriSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    LCaseValueUnaryDispatcher,
    LCaseSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    IntegerTermValueEncoder,
    StrLenValueUnaryDispatcher,
    StrLenSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    StringLiteralRefTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    UCaseValueUnaryDispatcher,
    UCaseSparqlOp
);

// Terms
impl_unary_sparql_op!(
    TermValueEncoding,
    SimpleLiteralRefTermValueDecoder,
    BlankNodeRefTermValueEncoder,
    BNodeValueUnaryDispatcher,
    BNodeSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    NamedNodeRefTermValueEncoder,
    DatatypeValueUnaryDispatcher,
    DatatypeSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    NamedNodeTermValueEncoder,
    IriValueUnaryDispatcher,
    IriSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    BooleanTermValueEncoder,
    IsBlankValueUnaryDispatcher,
    IsBlankSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    BooleanTermValueEncoder,
    IsIriValueUnaryDispatcher,
    IsIriSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    BooleanTermValueEncoder,
    IsLiteralValueUnaryDispatcher,
    IsLiteralSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    BooleanTermValueEncoder,
    IsNumericValueUnaryDispatcher,
    IsNumericSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    SimpleLiteralRefTermValueEncoder,
    LangValueUnaryDispatcher,
    LangSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    DefaultTermValueDecoder,
    OwnedStringLiteralTermValueEncoder,
    StrValueUnaryDispatcher,
    StrSparqlOp
);

pub(crate) fn dispatch_unary_array<'data, TEncoding, TDecoder, TEncoder, TOp>(
    op: &TOp,
    values: &'data TEncoding::Array,
) -> DFResult<ColumnarValue>
where
    TOp: UnaryTermValueOp,
    TEncoding: TermEncoding,
    TDecoder: TermDecoder<TEncoding, Term<'data> = TOp::Arg<'data>>,
    TEncoder: TermEncoder<TEncoding, Term<'data> = TOp::Result<'data>>,
{
    let results = <TDecoder>::decode_terms(values)
        .map(|v| match v {
            Ok(value) => op.evaluate(value),
            Err(ThinError::Expected) => op.evaluate_error(),
            Err(internal_err) => Err(internal_err),
        });
    let result = <TEncoder>::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

pub(crate) fn dispatch_unary_scalar<'data, TEncoding, TDecoder, TEncoder, TOp>(
    op: &TOp,
    value: &'data TEncoding::Scalar,
) -> DFResult<ColumnarValue>
where
    TOp: UnaryTermValueOp,
    TEncoding: TermEncoding,
    TDecoder: TermDecoder<TEncoding, Term<'data> = TOp::Arg<'data>>,
    TEncoder: TermEncoder<TEncoding, Term<'data> = TOp::Result<'data>>,
{
    let value = TDecoder::decode_term(value);
    let result = match value {
        Ok(value) => op.evaluate(value),
        Err(ThinError::Expected) => op.evaluate_error(),
        Err(ThinError::InternalError(error)) => {
            return exec_err!("InternalError in UDF: {}", error)
        }
    };
    let result = TEncoder::encode_term(result)?;
    Ok(ColumnarValue::Scalar(result.into_scalar_value()))
}
