use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::{ScalarSparqlOp, UnaryArgs};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::{SimpleLiteral, ThinError, TypedValue, TypedValueRef};
use sha2::{Digest, Sha256};

#[derive(Debug)]
pub struct Sha256SparqlOp;

impl Default for Sha256SparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha256SparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Sha256);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for Sha256SparqlOp {
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
        dispatch_unary_owned_typed_value(
            &arg,
            |value| {
                let string = match value {
                    TypedValueRef::SimpleLiteral(value) => value.value,
                    TypedValueRef::LanguageStringLiteral(value) => value.value,
                    _ => return ThinError::expected(),
                };

                let mut hasher = Sha256::new();
                hasher.update(string);
                let result = hasher.finalize();
                let value = format!("{:x}", result);

                Ok(TypedValue::SimpleLiteral(SimpleLiteral { value }))
            },
            || ThinError::expected(),
        )
    }
}
