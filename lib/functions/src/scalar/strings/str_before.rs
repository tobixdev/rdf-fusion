use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::{BinaryArgs, ScalarSparqlOp};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::{
    CompatibleStringArgs, LanguageStringRef, SimpleLiteralRef, StringLiteralRef, ThinError,
    TypedValueRef,
};

/// Implementation of the SPARQL `strbefore` function.
#[derive(Debug)]
pub struct StrBeforeSparqlOp {}

impl Default for StrBeforeSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrBeforeSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::StrBefore);

    /// Creates a new [StrBeforeSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for StrBeforeSparqlOp {
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
        dispatch_binary_typed_value(
            &lhs,
            &rhs,
            |lhs_value, rhs_value| {
                let lhs_value = StringLiteralRef::try_from(lhs_value)?;
                let rhs_value = StringLiteralRef::try_from(rhs_value)?;

                let args = CompatibleStringArgs::try_from(lhs_value, rhs_value)?;

                let value = if let Some(position) = args.lhs.find(args.rhs) {
                    &args.lhs[..position]
                } else {
                    return Ok(TypedValueRef::SimpleLiteral(SimpleLiteralRef { value: "" }));
                };

                Ok(match args.language {
                    None => TypedValueRef::SimpleLiteral(SimpleLiteralRef { value }),
                    Some(language) => {
                        TypedValueRef::LanguageStringLiteral(LanguageStringRef { value, language })
                    }
                })
            },
            |_, _| ThinError::expected(),
        )
    }
}
