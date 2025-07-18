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
pub struct EncodeForUriSparqlOp;

impl Default for EncodeForUriSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl EncodeForUriSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::EncodeForUri);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for EncodeForUriSparqlOp {
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
                    let string = match value {
                        TypedValueRef::SimpleLiteral(value) => value.value,
                        TypedValueRef::LanguageStringLiteral(value) => value.value,
                        _ => return ThinError::expected(),
                    };

                    // Based on oxigraph/lib/spareval/src/eval.rs
                    // Maybe we can use a library in the future?
                    let mut result = Vec::with_capacity(string.len());
                    for c in string.bytes() {
                        match c {
                            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                                result.push(c)
                            }
                            _ => {
                                result.push(b'%');
                                let high = c / 16;
                                let low = c % 16;
                                result.push(if high < 10 {
                                    b'0' + high
                                } else {
                                    b'A' + (high - 10)
                                });
                                result.push(if low < 10 {
                                    b'0' + low
                                } else {
                                    b'A' + (low - 10)
                                });
                            }
                        }
                    }

                    let value = String::from_utf8(result)?;
                    Ok(TypedValue::SimpleLiteral(SimpleLiteral { value }))
                },
                ThinError::expected,
            )
        }))
    }
}
