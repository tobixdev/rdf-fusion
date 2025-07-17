use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::{ScalarSparqlOp, SparqlOpSignature, UnarySparqlOpArgs, UnarySparqlOpSignature};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::{ThinError, TypedValueRef};

#[derive(Debug)]
pub struct IsNumericSparqlOp {}

impl Default for IsNumericSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IsNumericSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::IsNumeric);
    const SIGNATURE: UnarySparqlOpSignature = UnarySparqlOpSignature;

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for IsNumericSparqlOp {
    type Encoding = TypedValueEncoding;
    type Signature = UnarySparqlOpSignature;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> &Self::Signature {
        &Self::SIGNATURE
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn return_type(&self, target_encoding: Option<EncodingName>) -> DFResult<DataType> {
        if !matches!(target_encoding, Some(EncodingName::TypedValue)) {
            return exec_err!("Unexpected target encoding: {:?}", target_encoding);
        }
        Ok(TypedValueEncoding::data_type())
    }

    fn invoke(
        &self,
        UnarySparqlOpArgs(arg): <Self::Signature as SparqlOpSignature<Self::Encoding>>::Args,
    ) -> DFResult<ColumnarValue> {
        dispatch_unary_typed_value(
            &arg,
            |value| {
                Ok(TypedValueRef::BooleanLiteral(
                    matches!(value, TypedValueRef::NumericLiteral(_)).into(),
                ))
            },
            || ThinError::expected(),
        )
    }
}
