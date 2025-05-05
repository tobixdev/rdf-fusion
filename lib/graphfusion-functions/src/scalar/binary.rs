use crate::dispatcher::SparqlOpDispatcher;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, Signature};
use graphfusion_encoding::value_encoding::TermValueEncoding;
use graphfusion_encoding::TermDecoder;
use graphfusion_encoding::{EncodingArray, EncodingScalar};
use graphfusion_encoding::{TermEncoder, TermEncoding};
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
                Ok(<$ENCODING>::data_type())
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
                match args.args.as_slice() {
                    [ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)] => {
                        dispatch_binary_array_array::<$ENCODING, $SPARQL_OP>(
                            &self.op,
                            &<$ENCODING>::try_new_array(lhs.clone())?,
                            &<$ENCODING>::try_new_array(rhs.clone())?,
                        )
                    }
                    [ColumnarValue::Scalar(lhs), ColumnarValue::Array(rhs)] => {
                        dispatch_binary_scalar_array::<$ENCODING, $SPARQL_OP>(
                            &self.op,
                            &<$ENCODING>::try_new_scalar(lhs.clone())?,
                            &<$ENCODING>::try_new_array(rhs.clone())?,
                        )
                    }
                    [ColumnarValue::Array(lhs), ColumnarValue::Scalar(rhs)] => {
                        dispatch_binary_array_scalar::<$ENCODING, $SPARQL_OP>(
                            &self.op,
                            &<$ENCODING>::try_new_array(lhs.clone())?,
                            &<$ENCODING>::try_new_scalar(rhs.clone())?,
                        )
                    }
                    [ColumnarValue::Scalar(lhs), ColumnarValue::Scalar(rhs)] => {
                        dispatch_binary_scalar_scalar::<$ENCODING, $SPARQL_OP>(
                            &self.op,
                            &<$ENCODING>::try_new_scalar(lhs.clone())?,
                            &<$ENCODING>::try_new_scalar(rhs.clone())?,
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

fn dispatch_binary_array_array<'data, TEncoding, TOp>(
    op: &TOp,
    lhs: &'data TEncoding::Array,
    rhs: &'data TEncoding::Array,
) -> DFResult<ColumnarValue>
where
    TOp: BinaryTermValueOp,
    TEncoding: TermEncoding,
    TEncoding: TermDecoder<'data, TOp::ArgLhs<'data>>,
    TEncoding: TermDecoder<'data, TOp::ArgRhs<'data>>,
    TEncoding: TermEncoder<TOp::Result<'data>>,
{
    let lhs = <TEncoding as TermDecoder<TOp::ArgLhs<'data>>>::decode_terms(lhs);
    let rhs = <TEncoding as TermDecoder<TOp::ArgRhs<'data>>>::decode_terms(rhs);

    let results = lhs.zip(rhs).map(|(lhs, rhs)| match (lhs, rhs) {
        (Ok(lhs), Ok(rhs)) => op.evaluate(lhs, rhs),
        _ => op.evaluate_error(lhs, rhs),
    });
    let result = <TEncoding as TermEncoder<TOp::Result<'data>>>::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

fn dispatch_binary_scalar_array<'data, TEncoding, TOp>(
    op: &TOp,
    lhs: &'data TEncoding::Scalar,
    rhs: &'data TEncoding::Array,
) -> DFResult<ColumnarValue>
where
    TOp: BinaryTermValueOp,
    TEncoding: TermEncoding,
    TEncoding: TermDecoder<'data, TOp::ArgLhs<'data>>,
    TEncoding: TermDecoder<'data, TOp::ArgRhs<'data>>,
    TEncoding: TermEncoder<TOp::Result<'data>>,
{
    let lhs_value = <TEncoding as TermDecoder<TOp::ArgLhs<'data>>>::decode_term(lhs);

    let results =
        <TEncoding as TermDecoder<TOp::ArgRhs<'data>>>::decode_terms(rhs).map(|rhs_value| {
            match (lhs_value, rhs_value) {
                (Ok(lhs_value), Ok(rhs_value)) => op.evaluate(lhs_value, rhs_value),
                _ => op.evaluate_error(lhs_value, rhs_value),
            }
        });
    let result = <TEncoding as TermEncoder<TOp::Result<'data>>>::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

fn dispatch_binary_array_scalar<'data, TEncoding, TOp>(
    op: &TOp,
    lhs: &'data TEncoding::Array,
    rhs: &'data TEncoding::Scalar,
) -> DFResult<ColumnarValue>
where
    TOp: BinaryTermValueOp,
    TEncoding: TermEncoding,
    TEncoding: TermDecoder<'data, TOp::ArgLhs<'data>>,
    TEncoding: TermDecoder<'data, TOp::ArgRhs<'data>>,
    TEncoding: TermEncoder<TOp::Result<'data>>,
{
    let rhs_value = <TEncoding as TermDecoder<TOp::ArgRhs<'data>>>::decode_term(rhs);

    let results =
        <TEncoding as TermDecoder<TOp::ArgLhs<'data>>>::decode_terms(lhs).map(|lhs_value| {
            match (lhs_value, rhs_value) {
                (Ok(lhs_value), Ok(rhs_value)) => op.evaluate(lhs_value, rhs_value),
                _ => op.evaluate_error(lhs_value, rhs_value),
            }
        });
    let result = <TEncoding as TermEncoder<TOp::Result<'data>>>::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

fn dispatch_binary_scalar_scalar<'data, TEncoding, TOp>(
    op: &TOp,
    lhs: &'data TEncoding::Scalar,
    rhs: &'data TEncoding::Scalar,
) -> DFResult<ColumnarValue>
where
    TOp: BinaryTermValueOp,
    TEncoding: TermEncoding,
    TEncoding: TermDecoder<'data, TOp::ArgLhs<'data>>,
    TEncoding: TermDecoder<'data, TOp::ArgRhs<'data>>,
    TEncoding: TermEncoder<TOp::Result<'data>>,
{
    let lhs = <TEncoding as TermDecoder<TOp::ArgLhs<'data>>>::decode_term(lhs);
    let rhs = <TEncoding as TermDecoder<TOp::ArgRhs<'data>>>::decode_term(rhs);

    let result = match (lhs, rhs) {
        (Ok(lhs), Ok(rhs)) => {
            let result = op.evaluate(lhs, rhs);
            <TEncoding as TermEncoder<TOp::Result<'data>>>::encode_term(result)
        }
        _ => {
            let result = op.evaluate_error(lhs, rhs);
            <TEncoding as TermEncoder<TOp::Result<'data>>>::encode_term(result)
        }
    };
    Ok(ColumnarValue::Scalar(result?.into_scalar_value()))
}
