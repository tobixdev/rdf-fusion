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
use rdf_fusion_model::{ThinError, TypedValueRef};

#[derive(Debug)]
pub struct IsBlankSparqlOp {}

impl Default for IsBlankSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IsBlankSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::IsBlank);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for IsBlankSparqlOp {
    type Args<TEncoding: TermEncoding> = UnaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn supported_encodings(&self) -> &[EncodingName] {
        &[EncodingName::TypedValue]
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

    fn invoke_typed_value_encoding(
        &self,
        UnaryArgs(arg): Self::Args<TypedValueEncoding>,
    ) -> DFResult<ColumnarValue> {
        dispatch_unary_typed_value(
            &arg,
            |value| {
                Ok(TypedValueRef::BooleanLiteral(
                    matches!(value, TypedValueRef::BlankNode(_)).into(),
                ))
            },
            || ThinError::expected(),
        )
    }
}
