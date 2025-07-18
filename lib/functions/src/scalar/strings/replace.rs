use crate::builtin::BuiltinName;
use crate::scalar::dispatch::{
    dispatch_quaternary_owned_typed_value, dispatch_ternary_owned_typed_value,
};
use crate::scalar::strings::regex::compile_pattern;
use crate::scalar::{QuaternaryArgs, ScalarSparqlOp, TernaryArgs, TernaryOrQuaternaryArgs};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::{
    LanguageString, SimpleLiteral, SimpleLiteralRef, StringLiteralRef, ThinError, TypedValue,
    TypedValueRef,
};
use std::borrow::Cow;

/// Implementation of the SPARQL `regex` function (binary version).
#[derive(Debug)]
pub struct ReplaceSparqlOp {}

impl Default for ReplaceSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplaceSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Replace);

    /// Creates a new [ReplaceSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for ReplaceSparqlOp {
    type Args<TEncoding: TermEncoding> = TernaryOrQuaternaryArgs<TEncoding>;

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
        args: Self::Args<TypedValueEncoding>,
    ) -> DFResult<ColumnarValue> {
        match args {
            TernaryOrQuaternaryArgs::Ternary(TernaryArgs(arg0, arg1, arg2)) => {
                dispatch_ternary_owned_typed_value(
                    &arg0,
                    &arg1,
                    &arg2,
                    |arg0, arg1, arg2| evaluate_replace(arg0, arg1, arg2, None)?,
                    |_, _, _| ThinError::expected(),
                )
            }
            TernaryOrQuaternaryArgs::Quaternary(QuaternaryArgs(arg0, arg1, arg2, arg3)) => {
                dispatch_quaternary_owned_typed_value(
                    &arg0,
                    &arg1,
                    &arg2,
                    &arg3,
                    |arg0, arg1, arg2, arg3| evaluate_replace(arg0, arg1, arg2, Some(arg3))?,
                    |_, _, _, _| ThinError::expected(),
                )
            }
        }
    }
}

fn evaluate_replace(
    arg0: TypedValueRef<'_>,
    arg1: TypedValueRef<'_>,
    arg2: TypedValueRef<'_>,
    arg3: Option<TypedValueRef<'_>>,
) -> Result<Result<TypedValue, ThinError>, ThinError> {
    let arg0 = StringLiteralRef::try_from(arg0)?;
    let arg1 = SimpleLiteralRef::try_from(arg1)?;
    let arg2 = SimpleLiteralRef::try_from(arg2)?;
    let arg3 = arg3.map(SimpleLiteralRef::try_from).transpose()?;

    let regex = compile_pattern(arg1.value, arg3.map(|lit| lit.value))?;

    let result = match regex.replace_all(arg0.0, arg2.value) {
        Cow::Owned(replaced) => replaced,
        Cow::Borrowed(_) => arg0.0.to_owned(),
    };

    Ok(Ok(match arg0.1 {
        None => TypedValue::SimpleLiteral(SimpleLiteral { value: result }),
        Some(language) => TypedValue::LanguageStringLiteral(LanguageString {
            value: result,
            language: language.to_owned(),
        }),
    }))
}
