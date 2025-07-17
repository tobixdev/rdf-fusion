use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::{ScalarSparqlOp, SparqlOpSignature, UnaryArgs, UnarySparqlOpSignature};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::{SimpleLiteral, ThinError, TypedValue, TypedValueRef};
use sha2::{Digest, Sha512};

#[derive(Debug)]
pub struct Sha512SparqlOp;

impl Default for Sha512SparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha512SparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Sha512);
    const SIGNATURE: UnarySparqlOpSignature = UnarySparqlOpSignature;

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for Sha512SparqlOp {
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

                let mut hasher = Sha512::new();
                hasher.update(string);
                let result = hasher.finalize();
                let value = format!("{:x}", result);

                Ok(TypedValue::SimpleLiteral(SimpleLiteral { value }))
            },
            || ThinError::expected(),
        )
    }
}
