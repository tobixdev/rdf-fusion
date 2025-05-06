use crate::dispatcher::SparqlOpDispatcher;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr_common::signature::Signature;
use graphfusion_encoding::value_encoding::TermValueEncoding;
use graphfusion_encoding::{
    DefaultDecoder, DefaultEncoderTerm, EncodingArray, EncodingScalar, TermDecoder, TermEncoder,
    TermEncoding,
};
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
use graphfusion_model::{RdfTermValueArg, ThinError};
use std::fmt::Debug;

macro_rules! impl_unary_sparql_op {
    ($ENCODING: ty, $STRUCT_NAME: ident, $SPARQL_OP: ty) => {
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
                        dispatch_unary_array::<$ENCODING, $SPARQL_OP>(
                            &self.op,
                            &<$ENCODING>::try_new_array(arg)?,
                        )
                    }
                    Ok([ColumnarValue::Scalar(arg)]) => {
                        dispatch_unary_scalar::<$ENCODING, $SPARQL_OP>(
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
    AsBooleanValueUnaryDispatcher,
    AsBooleanSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    AsDateTimeValueUnaryDispatcher,
    AsDateTimeSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    AsDecimalValueUnaryDispatcher,
    AsDecimalSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    AsDoubleValueUnaryDispatcher,
    AsDoubleSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    AsFloatValueUnaryDispatcher,
    AsFloatSparqlOp
);
impl_unary_sparql_op!(TermValueEncoding, AsIntValueUnaryDispatcher, AsIntSparqlOp);
impl_unary_sparql_op!(
    TermValueEncoding,
    AsIntegerValueUnaryDispatcher,
    AsIntegerSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    AsStringValueUnaryDispatcher,
    AsStringSparqlOp
);

// Dates and Times
impl_unary_sparql_op!(TermValueEncoding, DayValueUnaryDispatcher, DaySparqlOp);
impl_unary_sparql_op!(TermValueEncoding, HoursValueUnaryDispatcher, HoursSparqlOp);
impl_unary_sparql_op!(
    TermValueEncoding,
    MinutesValueUnaryDispatcher,
    MinutesSparqlOp
);
impl_unary_sparql_op!(TermValueEncoding, MonthValueUnaryDispatcher, MonthSparqlOp);
impl_unary_sparql_op!(
    TermValueEncoding,
    SecondsValueUnaryDispatcher,
    SecondsSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    TimezoneValueUnaryDispatcher,
    TimezoneSparqlOp
);
impl_unary_sparql_op!(TermValueEncoding, TzValueUnaryDispatcher, TzSparqlOp);
impl_unary_sparql_op!(TermValueEncoding, YearValueUnaryDispatcher, YearSparqlOp);

// Functional Form
impl_unary_sparql_op!(TermValueEncoding, BoundValueUnaryDispatcher, BoundSparqlOp);

// Hashing
impl_unary_sparql_op!(TermValueEncoding, Md5ValueUnaryDispatcher, Md5SparqlOp);
impl_unary_sparql_op!(TermValueEncoding, Sha1ValueUnaryDispatcher, Sha1SparqlOp);
impl_unary_sparql_op!(
    TermValueEncoding,
    Sha256ValueUnaryDispatcher,
    Sha256SparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    Sha384ValueUnaryDispatcher,
    Sha384SparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    Sha512ValueUnaryDispatcher,
    Sha512SparqlOp
);

// Numeric
impl_unary_sparql_op!(TermValueEncoding, AbsValueUnaryDispatcher, AbsSparqlOp);
impl_unary_sparql_op!(TermValueEncoding, CeilValueUnaryDispatcher, CeilSparqlOp);
impl_unary_sparql_op!(TermValueEncoding, FloorValueUnaryDispatcher, FloorSparqlOp);
impl_unary_sparql_op!(TermValueEncoding, RoundValueUnaryDispatcher, RoundSparqlOp);
impl_unary_sparql_op!(
    TermValueEncoding,
    UnaryMinusValueUnaryDispatcher,
    UnaryMinusSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    UnaryPlusValueUnaryDispatcher,
    UnaryPlusSparqlOp
);

// Strings
impl_unary_sparql_op!(
    TermValueEncoding,
    EncodeForUriValueUnaryDispatcher,
    EncodeForUriSparqlOp
);
impl_unary_sparql_op!(TermValueEncoding, LCaseValueUnaryDispatcher, LCaseSparqlOp);
impl_unary_sparql_op!(
    TermValueEncoding,
    StrLenValueUnaryDispatcher,
    StrLenSparqlOp
);
impl_unary_sparql_op!(TermValueEncoding, UCaseValueUnaryDispatcher, UCaseSparqlOp);

// Terms
impl_unary_sparql_op!(TermValueEncoding, BNodeValueUnaryDispatcher, BNodeSparqlOp);
impl_unary_sparql_op!(
    TermValueEncoding,
    DatatypeValueUnaryDispatcher,
    DatatypeSparqlOp
);
impl_unary_sparql_op!(TermValueEncoding, IriValueUnaryDispatcher, IriSparqlOp);
impl_unary_sparql_op!(
    TermValueEncoding,
    IsBlankValueUnaryDispatcher,
    IsBlankSparqlOp
);
impl_unary_sparql_op!(TermValueEncoding, IsIriValueUnaryDispatcher, IsIriSparqlOp);
impl_unary_sparql_op!(
    TermValueEncoding,
    IsLiteralValueUnaryDispatcher,
    IsLiteralSparqlOp
);
impl_unary_sparql_op!(
    TermValueEncoding,
    IsNumericValueUnaryDispatcher,
    IsNumericSparqlOp
);
impl_unary_sparql_op!(TermValueEncoding, LangValueUnaryDispatcher, LangSparqlOp);
impl_unary_sparql_op!(TermValueEncoding, StrValueUnaryDispatcher, StrSparqlOp);

fn dispatch_unary_array<'data, TEncoding, TOp>(
    op: &TOp,
    values: &'data TEncoding::Array,
) -> DFResult<ColumnarValue>
where
    TOp: UnaryTermValueOp,
    TEncoding: TermEncoding,
    TOp::Result<'data>: Into<DefaultEncoderTerm<'data, TEncoding>>,
{
    let results = <DefaultDecoder<TEncoding>>::decode_terms(values)
        .map(|res| res.and_then(|term| TOp::Arg::try_from_value(term)))
        .map(|v| match v {
            Ok(value) => op.evaluate(value),
            Err(ThinError::Expected) => op.evaluate_error(),
            Err(internal_err) => Err(internal_err),
        });
    let result = <TEncoding as TermEncoder<TOp::Result<'data>>>::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

fn dispatch_unary_scalar<'data, TEncoding, TOp>(
    op: &TOp,
    value: &'data TEncoding::Scalar,
) -> DFResult<ColumnarValue>
where
    TOp: UnaryTermValueOp,
    TEncoding: TermEncoding,
    TOp::Result<'data>: Into<DefaultEncoderTerm<'data, TEncoding>>,
{
    let value = <DefaultDecoder<TEncoding>>::decode_term(value);
    let result = match value {
        Ok(value) => op.evaluate(value),
        Err(ThinError::Expected) => op.evaluate_error(),
        Err(ThinError::InternalError(error)) => {
            return exec_err!("InternalError in UDF: {}", error)
        }
    };
    let result = <TEncoding as TermEncoder<TOp::Result<'data>>>::encode_term(result)?;
    Ok(ColumnarValue::Scalar(result.into_scalar_value()))
}
