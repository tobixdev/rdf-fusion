use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::{ScalarSparqlOp, UnaryArgs};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::{Boolean, Numeric, ThinError, TypedValueRef};

#[derive(Debug)]
pub struct CastBooleanSparqlOp;

impl Default for CastBooleanSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CastBooleanSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::CastBoolean);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for CastBooleanSparqlOp {
    type Args<TEncoding: TermEncoding> = UnaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn return_type(&self, input_encoding: Option<EncodingName>) -> DFResult<DataType> {
        if !matches!(input_encoding, Some(EncodingName::TypedValue)) {
            return exec_err!("Unexpected target encoding: {:?}", input_encoding);
        }
        Ok(TypedValueEncoding::data_type())
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn Fn(Self::Args<TypedValueEncoding>) -> DFResult<ColumnarValue>>> {
        Some(Box::new(|UnaryArgs(arg)| {
            dispatch_unary_typed_value(
                &arg,
                |value| {
                    let converted = match value {
                        TypedValueRef::BooleanLiteral(v) => v,
                        TypedValueRef::SimpleLiteral(v) => v.value.parse()?,
                        TypedValueRef::NumericLiteral(numeric) => match numeric {
                            Numeric::Int(v) => Boolean::from(v),
                            Numeric::Integer(v) => Boolean::from(v),
                            Numeric::Float(v) => Boolean::from(v),
                            Numeric::Double(v) => Boolean::from(v),
                            Numeric::Decimal(v) => Boolean::from(v),
                        },
                        _ => return ThinError::expected(),
                    };
                    Ok(TypedValueRef::from(converted))
                },
                ThinError::expected,
            )
        }))
    }
}
