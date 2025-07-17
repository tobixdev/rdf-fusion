use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::{ScalarSparqlOp, SparqlOpSignature, UnaryArgs, UnarySparqlOpSignature};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use md5::{Digest, Md5};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::{SimpleLiteral, ThinError, TypedValue, TypedValueRef};

#[derive(Debug)]
pub struct Md5SparqlOp;

impl Default for Md5SparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Md5SparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Md5);
    const SIGNATURE: UnarySparqlOpSignature = UnarySparqlOpSignature;

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for Md5SparqlOp {
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
        UnaryArgs(arg): <Self::Signature as SparqlOpSignature<Self::Encoding>>::Args,
    ) -> DFResult<ColumnarValue> {
        dispatch_unary_owned_typed_value(
            &arg,
            |value| {
                let string = match value {
                    TypedValueRef::SimpleLiteral(value) => value.value,
                    TypedValueRef::LanguageStringLiteral(value) => value.value,
                    _ => return ThinError::expected(),
                };

                let mut hasher = Md5::new();
                hasher.update(string);
                let result = hasher.finalize();
                let value = format!("{:x}", result);

                Ok(TypedValue::SimpleLiteral(SimpleLiteral { value }))
            },
            || ThinError::expected(),
        )
    }
}