use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_binary_owned_typed_value;
use crate::scalar::{BinaryArgs, ScalarSparqlOp};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::{LanguageString, ThinError, TypedValue, TypedValueRef};

/// TODO
#[derive(Debug)]
pub struct StrLangSparqlOp {}

impl Default for StrLangSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrLangSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::StrLang);

    /// Creates a new [StrLangSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for StrLangSparqlOp {
    type Args<TEncoding: TermEncoding> = BinaryArgs<TEncoding>;

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
        BinaryArgs(lhs, rhs): Self::Args<TypedValueEncoding>,
    ) -> DFResult<ColumnarValue> {
        dispatch_binary_owned_typed_value(
            &lhs,
            &rhs,
            |lhs_value, rhs_value| {
                if let (
                    TypedValueRef::SimpleLiteral(lhs_literal),
                    TypedValueRef::SimpleLiteral(rhs_literal),
                ) = (lhs_value, rhs_value)
                {
                    Ok(TypedValue::LanguageStringLiteral(LanguageString {
                        value: lhs_literal.value.to_owned(),
                        language: rhs_literal.value.to_ascii_lowercase(),
                    }))
                } else {
                    ThinError::expected()
                }
            },
            |_, _| ThinError::expected(),
        )
    }
}
