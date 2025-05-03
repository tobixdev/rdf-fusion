use crate::dispatcher::SparqlOpDispatcher;
use crate::DFResult;
use datafusion::arrow::array::{Array, AsArray, BooleanArray};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::common::{exec_datafusion_err, exec_err, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::ScalarFunctionArgs;
use datafusion::logical_expr_common::signature::Signature;
use graphfusion_encoding::value_encoding::{TermValueEncoding, ValueEncodingField};
use graphfusion_encoding::{as_term_value_array, EncodingScalar, FromArrow, ScalarEncoder, TermEncoding, ToArrow};
use graphfusion_functions_scalar::{
    AbsSparqlOp, AsDecimalSparqlOp, AsDoubleSparqlOp, AsFloatSparqlOp, AsIntSparqlOp,
    AsIntegerSparqlOp, AsStringSparqlOp, BNodeSparqlOp, BoundSparqlOp, CeilSparqlOp,
    DatatypeSparqlOp, DaySparqlOp, EncodeForUriSparqlOp, FloorSparqlOp, HoursSparqlOp, IriSparqlOp,
    IsBlankSparqlOp, IsIriSparqlOp, IsLiteralSparqlOp, IsNumericSparqlOp, LCaseSparqlOp,
    LangSparqlOp, Md5SparqlOp, MinutesSparqlOp, MonthSparqlOp, RoundSparqlOp, SecondsSparqlOp,
    Sha1SparqlOp, Sha256SparqlOp, Sha384SparqlOp, Sha512SparqlOp, SparqlOp, StrLenSparqlOp,
    StrSparqlOp, TimezoneSparqlOp, TzSparqlOp, UCaseSparqlOp, UnaryMinusSparqlOp,
    UnaryPlusSparqlOp, YearSparqlOp,
};
use graphfusion_functions_scalar::{AsBooleanSparqlOp, AsDateTimeSparqlOp, UnaryTermValueOp};
use model::{Boolean, RdfTermValueArg, TermValueRef, ThinError};
use std::fmt::Debug;

macro_rules! impl_unary_rdf_value_op {
    ($STRUCT_NAME: ident, $SPARQL_OP: ty) => {
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
                Ok(<$SPARQL_OP as UnaryTermValueOp>::Result::encoded_datatype())
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
                match &args.args.as_slice() {
                    [ColumnarValue::Array(arg)] => dispatch_unary_array(&self.op, arg),
                    [ColumnarValue::Scalar(arg)] => dispatch_unary_scalar(&self.op, arg),
                    _ => Err(DataFusionError::Execution(String::from(
                        "Unexpected type combination.",
                    ))),
                }
            }
        }
    };
}

// Conversion
impl_unary_rdf_value_op!(AsBooleanValueUnaryDispatcher, AsBooleanSparqlOp);
impl_unary_rdf_value_op!(AsDateTimeValueUnaryDispatcher, AsDateTimeSparqlOp);
impl_unary_rdf_value_op!(AsDecimalValueUnaryDispatcher, AsDecimalSparqlOp);
impl_unary_rdf_value_op!(AsDoubleValueUnaryDispatcher, AsDoubleSparqlOp);
impl_unary_rdf_value_op!(AsFloatValueUnaryDispatcher, AsFloatSparqlOp);
impl_unary_rdf_value_op!(AsIntValueUnaryDispatcher, AsIntSparqlOp);
impl_unary_rdf_value_op!(AsIntegerValueUnaryDispatcher, AsIntegerSparqlOp);
impl_unary_rdf_value_op!(AsStringValueUnaryDispatcher, AsStringSparqlOp);

// Dates and Times
impl_unary_rdf_value_op!(DayValueUnaryDispatcher, DaySparqlOp);
impl_unary_rdf_value_op!(HoursValueUnaryDispatcher, HoursSparqlOp);
impl_unary_rdf_value_op!(MinutesValueUnaryDispatcher, MinutesSparqlOp);
impl_unary_rdf_value_op!(MonthValueUnaryDispatcher, MonthSparqlOp);
impl_unary_rdf_value_op!(SecondsValueUnaryDispatcher, SecondsSparqlOp);
impl_unary_rdf_value_op!(TimezoneValueUnaryDispatcher, TimezoneSparqlOp);
impl_unary_rdf_value_op!(TzValueUnaryDispatcher, TzSparqlOp);
impl_unary_rdf_value_op!(YearValueUnaryDispatcher, YearSparqlOp);

// Functional Form
impl_unary_rdf_value_op!(BoundValueUnaryDispatcher, BoundSparqlOp);

// Hashing
impl_unary_rdf_value_op!(Md5ValueUnaryDispatcher, Md5SparqlOp);
impl_unary_rdf_value_op!(Sha1ValueUnaryDispatcher, Sha1SparqlOp);
impl_unary_rdf_value_op!(Sha256ValueUnaryDispatcher, Sha256SparqlOp);
impl_unary_rdf_value_op!(Sha384ValueUnaryDispatcher, Sha384SparqlOp);
impl_unary_rdf_value_op!(Sha512ValueUnaryDispatcher, Sha512SparqlOp);

// Numeric
impl_unary_rdf_value_op!(AbsValueUnaryDispatcher, AbsSparqlOp);
impl_unary_rdf_value_op!(CeilValueUnaryDispatcher, CeilSparqlOp);
impl_unary_rdf_value_op!(FloorValueUnaryDispatcher, FloorSparqlOp);
impl_unary_rdf_value_op!(RoundValueUnaryDispatcher, RoundSparqlOp);
impl_unary_rdf_value_op!(UnaryMinusValueUnaryDispatcher, UnaryMinusSparqlOp);
impl_unary_rdf_value_op!(UnaryPlusValueUnaryDispatcher, UnaryPlusSparqlOp);

// Strings
impl_unary_rdf_value_op!(EncodeForUriValueUnaryDispatcher, EncodeForUriSparqlOp);
impl_unary_rdf_value_op!(LCaseValueUnaryDispatcher, LCaseSparqlOp);
impl_unary_rdf_value_op!(StrLenValueUnaryDispatcher, StrLenSparqlOp);
impl_unary_rdf_value_op!(UCaseValueUnaryDispatcher, UCaseSparqlOp);

// Terms
impl_unary_rdf_value_op!(BNodeValueUnaryDispatcher, BNodeSparqlOp);
impl_unary_rdf_value_op!(DatatypeValueUnaryDispatcher, DatatypeSparqlOp);
impl_unary_rdf_value_op!(IriValueUnaryDispatcher, IriSparqlOp);
impl_unary_rdf_value_op!(IsBlankValueUnaryDispatcher, IsBlankSparqlOp);
impl_unary_rdf_value_op!(IsIriValueUnaryDispatcher, IsIriSparqlOp);
impl_unary_rdf_value_op!(IsLiteralValueUnaryDispatcher, IsLiteralSparqlOp);
impl_unary_rdf_value_op!(IsNumericValueUnaryDispatcher, IsNumericSparqlOp);
impl_unary_rdf_value_op!(LangValueUnaryDispatcher, LangSparqlOp);
impl_unary_rdf_value_op!(StrValueUnaryDispatcher, StrSparqlOp);

fn dispatch_unary_scalar<'data, TOp>(op: &TOp, value: &'data ScalarValue) -> DFResult<ColumnarValue>
where
    TOp: UnaryTermValueOp,
    TOp::Arg<'data>: FromArrow<'data>,
    TOp::Result<'data>: ToArrow,
{
    let value = TOp::Arg::from_scalar(value);
    let result = match value {
        Ok(value) => op.evaluate(value),
        Err(ThinError::Expected) => op.evaluate_error(),
        Err(ThinError::InternalError(error)) => {
            return exec_err!("InternalError in UDF: {}", error)
        }
    };
    let result = match result {
        Ok(result) => result.into_scalar_value(),
        Err(_) => Ok(<TermValueEncoding as TermEncoding>::ScalarEncoder::encode_scalar_null().into_scalar_value()),
    };
    Ok(ColumnarValue::Scalar(result?))
}

fn dispatch_unary_array<'data, TOp>(op: &TOp, values: &'data dyn Array) -> DFResult<ColumnarValue>
where
    TOp: UnaryTermValueOp,
    TOp::Arg<'data>: FromArrow<'data>,
    TOp::Result<'data>: ToArrow,
{
    let values = as_term_value_array(values)?;

    let booleans = values
        .child(ValueEncodingField::Boolean.type_id())
        .as_boolean();
    if values.len() == booleans.len() {
        let offsets = values.offsets().ok_or(exec_datafusion_err!(
            "RDF term array should always have offsets."
        ))?;
        return dispatch_unary_array_boolean(op, offsets, booleans);
    }

    let results = (0..values.len())
        .map(|i| TermValueRef::from_array(values, i).and_then(TOp::Arg::try_from_value))
        .map(|v| match v {
            Ok(value) => op.evaluate(value),
            Err(ThinError::Expected) => op.evaluate_error(),
            Err(internal_err) => Err(internal_err),
        });
    let result = TOp::Result::iter_into_array(results)?;
    Ok(ColumnarValue::Array(result))
}

#[inline(never)]
fn dispatch_unary_array_boolean<'data, TOp>(
    op: &TOp,
    offsets: &ScalarBuffer<i32>,
    values: &BooleanArray,
) -> DFResult<ColumnarValue>
where
    TOp: UnaryTermValueOp,
    TOp::Arg<'data>: FromArrow<'data>,
    TOp::Result<'data>: ToArrow,
{
    #[allow(clippy::cast_sign_loss, reason = "Offset is always positive")]
    let results = offsets
        .iter()
        .map(|o| values.value(*o as usize))
        .map(|v| TOp::Arg::try_from_value(TermValueRef::BooleanLiteral(Boolean::from(v))))
        .map(|v| op.evaluate(v?));
    let result = TOp::Result::iter_into_array(results)?;
    Ok(ColumnarValue::Array(result))
}
