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

#[derive(Debug)]
pub struct LangSparqlOp;

impl Default for LangSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl LangSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Lang);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for LangSparqlOp {
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
                ThinError::expected,
            )
        }))
    }
}
