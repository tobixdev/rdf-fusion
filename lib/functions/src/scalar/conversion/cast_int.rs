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
use rdf_fusion_model::{Int, Numeric, ThinError, TypedValueRef};

#[derive(Debug)]
pub struct CastIntSparqlOp;

impl Default for CastIntSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CastIntSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::AsInt);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for CastIntSparqlOp {
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
                        TypedValueRef::BooleanLiteral(v) => Int::from(v),
                        TypedValueRef::SimpleLiteral(v) => v.value.parse()?,
                        TypedValueRef::NumericLiteral(numeric) => match numeric {
                            Numeric::Int(v) => v,
                            Numeric::Integer(v) => Int::try_from(v)?,
                            Numeric::Float(v) => Int::try_from(v)?,
                            Numeric::Double(v) => Int::try_from(v)?,
                            Numeric::Decimal(v) => Int::try_from(v)?,
                        },
                        _ => return ThinError::expected(),
                    };
                    Ok(TypedValueRef::NumericLiteral(Numeric::from(converted)))
                },
                ThinError::expected,
            )
        }))
    }
}
