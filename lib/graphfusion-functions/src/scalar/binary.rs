use crate::dispatcher::SparqlOpDispatcher;
use crate::DFResult;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, Signature};
use graphfusion_encoding::{EncodingScalar, ScalarEncoder};
use graphfusion_encoding::value_encoding::TermValueEncoding;
use graphfusion_encoding::TermEncoding;
use graphfusion_encoding::{as_term_value_array, TermDecoder, FromArrow, ToArrow};
use graphfusion_functions_scalar::{
    AddSparqlOp, BinaryTermValueOp, ContainsSparqlOp, DivSparqlOp, EqSparqlOp,
    GreaterOrEqualSparqlOp, GreaterThanSparqlOp, LangMatchesSparqlOp, LessOrEqualSparqlOp,
    LessThanSparqlOp, MulSparqlOp, RegexSparqlOp, SameTermSparqlOp, SparqlOp, StrAfterSparqlOp,
    StrBeforeSparqlOp, StrDtSparqlOp, StrEndsSparqlOp, StrLangSparqlOp, StrStartsSparqlOp,
    SubSparqlOp, SubStrSparqlOp,
};

macro_rules! impl_unary_rdf_value_op {
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
                Ok(<$SPARQL_OP as BinaryTermValueOp>::Result::encoded_datatype())
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
                match args.args.as_slice() {
                    [ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)] => {
                        dispatch_binary_array_array(
                            &self.op,
                            &<$ENCODING>::try_new_array(lhs.clone())?,
                            &<$ENCODING>::try_new_array(rhs.clone())?,
                        )
                    }
                    [ColumnarValue::Scalar(lhs), ColumnarValue::Array(rhs)] => {
                        dispatch_binary_scalar_array(&self.op, lhs, rhs, args.number_rows)
                    }
                    [ColumnarValue::Array(lhs), ColumnarValue::Scalar(rhs)] => {
                        dispatch_binary_array_scalar(&self.op, lhs, rhs, args.number_rows)
                    }
                    [ColumnarValue::Scalar(lhs), ColumnarValue::Scalar(rhs)] => {
                        dispatch_binary_scalar_scalar::<$ENCODING, $SPARQL_OP>(&self.op, lhs, rhs)
                    }
                    _ => Err(DataFusionError::Execution(String::from(
                        "Unexpected type combination.",
                    ))),
                }
            }
        }
    };
}

// Comparisons
impl_unary_rdf_value_op!(
    TermValueEncoding,
    SameTermValueBinaryDispatcher,
    SameTermSparqlOp
);
impl_unary_rdf_value_op!(TermValueEncoding, EqValueBinaryDispatcher, EqSparqlOp);
impl_unary_rdf_value_op!(
    TermValueEncoding,
    GreaterThanValueBinaryDispatcher,
    GreaterThanSparqlOp
);
impl_unary_rdf_value_op!(
    TermValueEncoding,
    GreaterOrEqualValueBinaryDispatcher,
    GreaterOrEqualSparqlOp
);
impl_unary_rdf_value_op!(
    TermValueEncoding,
    LessThanValueBinaryDispatcher,
    LessThanSparqlOp
);
impl_unary_rdf_value_op!(
    TermValueEncoding,
    LessOrEqualValueBinaryDispatcher,
    LessOrEqualSparqlOp
);

// Numeric
impl_unary_rdf_value_op!(TermValueEncoding, AddValueBinaryDispatcher, AddSparqlOp);
impl_unary_rdf_value_op!(TermValueEncoding, DivValueBinaryDispatcher, DivSparqlOp);
impl_unary_rdf_value_op!(TermValueEncoding, MulValueBinaryDispatcher, MulSparqlOp);
impl_unary_rdf_value_op!(TermValueEncoding, SubValueBinaryDispatcher, SubSparqlOp);

// Strings
impl_unary_rdf_value_op!(
    TermValueEncoding,
    ContainsValueBinaryDispatcher,
    ContainsSparqlOp
);
impl_unary_rdf_value_op!(
    TermValueEncoding,
    LangMatchesValueBinaryDispatcher,
    LangMatchesSparqlOp
);
impl_unary_rdf_value_op!(TermValueEncoding, RegexValueBinaryDispatcher, RegexSparqlOp);
impl_unary_rdf_value_op!(
    TermValueEncoding,
    StrAfterValueBinaryDispatcher,
    StrAfterSparqlOp
);
impl_unary_rdf_value_op!(
    TermValueEncoding,
    StrBeforeValueBinaryDispatcher,
    StrBeforeSparqlOp
);
impl_unary_rdf_value_op!(
    TermValueEncoding,
    StrEndsValueBinaryDispatcher,
    StrEndsSparqlOp
);
impl_unary_rdf_value_op!(
    TermValueEncoding,
    StrStartsValueBinaryDispatcher,
    StrStartsSparqlOp
);
impl_unary_rdf_value_op!(
    TermValueEncoding,
    SubStrValueBinaryDispatcher,
    SubStrSparqlOp
);

// Terms
impl_unary_rdf_value_op!(TermValueEncoding, SubDtValueBinaryDispatcher, StrDtSparqlOp);
impl_unary_rdf_value_op!(
    TermValueEncoding,
    SubLangValueBinaryDispatcher,
    StrLangSparqlOp
);

fn dispatch_binary_array_array<'data, TArray, TOp>(
    op: &TOp,
    lhs: &'data TArray,
    rhs: &'data TArray,
) -> DFResult<ColumnarValue>
where
    TOp: BinaryTermValueOp,
    TArray: TermDecoder<'data, TOp::ArgLhs<'data>>,
    TArray: TermDecoder<'data, TOp::ArgRhs<'data>>,
    TOp::Result<'data>: ToArrow,
{
    let lhs = TermDecoder::<TOp::ArgLhs<'data>>::extract(lhs);
    let rhs = TermDecoder::<TOp::ArgRhs<'data>>::extract(rhs);

    let results = lhs.zip(rhs).map(|(lhs, rhs)| match (lhs, rhs) {
        (Ok(lhs), Ok(rhs)) => op.evaluate(lhs, rhs),
        _ => op.evaluate_error(lhs, rhs),
    });
    let result = TOp::Result::iter_into_array(results)?;
    Ok(ColumnarValue::Array(result))
}

fn dispatch_binary_scalar_array<'data, TOp>(
    op: &TOp,
    lhs: &'data ScalarValue,
    rhs: &'data dyn Array,
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TOp: BinaryTermValueOp,
    TOp::ArgLhs<'data>: FromArrow<'data>,
    TOp::ArgRhs<'data>: FromArrow<'data>,
    TOp::Result<'data>: ToArrow,
{
    let lhs_value = TOp::ArgLhs::from_scalar(lhs);

    let rhs = as_term_value_array(rhs)?;
    let results = (0..number_of_rows).map(|i| {
        let rhs_value = TOp::ArgRhs::from_array(rhs, i);
        match (lhs_value, rhs_value) {
            (Ok(lhs_value), Ok(rhs_value)) => op.evaluate(lhs_value, rhs_value),
            _ => op.evaluate_error(lhs_value, rhs_value),
        }
    });
    let result = TOp::Result::iter_into_array(results)?;
    Ok(ColumnarValue::Array(result))
}

fn dispatch_binary_array_scalar<'data, TOp>(
    op: &TOp,
    lhs: &'data dyn Array,
    rhs: &'data ScalarValue,
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TOp: BinaryTermValueOp,
    TOp::ArgLhs<'data>: FromArrow<'data>,
    TOp::ArgRhs<'data>: FromArrow<'data>,
    TOp::Result<'data>: ToArrow,
{
    let rhs_value = TOp::ArgRhs::from_scalar(rhs);

    let lhs = as_term_value_array(lhs)?;
    let results = (0..number_of_rows).map(|i| {
        let lhs_value = TOp::ArgLhs::from_array(lhs, i);
        match (lhs_value, rhs_value) {
            (Ok(lhs_value), Ok(rhs_value)) => op.evaluate(lhs_value, rhs_value),
            _ => op.evaluate_error(lhs_value, rhs_value),
        }
    });
    let result = TOp::Result::iter_into_array(results)?;
    Ok(ColumnarValue::Array(result))
}

fn dispatch_binary_scalar_scalar<'data, TEnccoding, TOp>(
    op: &TOp,
    lhs: &'data ScalarValue,
    rhs: &'data ScalarValue,
) -> DFResult<ColumnarValue>
where
    TEnccoding: TermEncoding,
    TOp: BinaryTermValueOp,
    TOp::ArgLhs<'data>: FromArrow<'data>,
    TOp::ArgRhs<'data>: FromArrow<'data>,
    TOp::Result<'data>: ToArrow,
{
    let lhs = TOp::ArgLhs::from_scalar(lhs);
    let rhs = TOp::ArgRhs::from_scalar(rhs);

    let result = match (lhs, rhs) {
        (Ok(lhs), Ok(rhs)) => match op.evaluate(lhs, rhs) {
            Ok(result) => result.into_scalar_value(),
            Err(_) => Ok(TEnccoding::ScalarEncoder::encode_scalar_null().into_scalar_value()),
        },
        _ => match op.evaluate_error(lhs, rhs) {
            Ok(result) => result.into_scalar_value(),
            Err(_) => Ok(TEnccoding::ScalarEncoder::encode_scalar_null().into_scalar_value()),
        },
    };
    Ok(ColumnarValue::Scalar(result?))
}
