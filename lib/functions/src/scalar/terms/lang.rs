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

#[derive(Debug)]
pub struct LangSparqlOp;

impl Default for LangSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl LangSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Lang);
    const SIGNATURE: UnarySparqlOpSignature = UnarySparqlOpSignature;

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for LangSparqlOp {
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
                let result = match value {
                    TypedValueRef::NamedNode(_) | TypedValueRef::BlankNode(_) => {
                        return ThinError::expected()
                    }
                    TypedValueRef::LanguageStringLiteral(value) => value.language,
                    _ => "",
                };
                Ok(TypedValue::SimpleLiteral(SimpleLiteral {
                    value: result.to_owned(),
                }))
            },
            || ThinError::expected(),
        )
    }
}
