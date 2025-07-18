use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::{ScalarSparqlOp, UnaryArgs};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::{ThinError, TypedValueRef};

#[derive(Debug)]
pub struct CastDateTimeSparqlOp;

impl Default for CastDateTimeSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CastDateTimeSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::CastDateTime);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for CastDateTimeSparqlOp {
    type Args<TEncoding: TermEncoding> = UnaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn return_type(&self, _input_encoding: Option<EncodingName>) -> DFResult<DataType> {
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
                        TypedValueRef::SimpleLiteral(v) => v.value.parse()?,
                        TypedValueRef::DateTimeLiteral(v) => v,
                        _ => return ThinError::expected(),
                    };
                    Ok(TypedValueRef::DateTimeLiteral(converted))
                },
                ThinError::expected,
            )
        }))
    }
}
