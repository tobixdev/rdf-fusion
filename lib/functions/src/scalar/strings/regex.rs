use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::{BinaryArgs, BinaryOrTernaryArgs, ScalarSparqlOp};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::{ThinError, ThinResult, TypedValueRef};
use regex::{Regex, RegexBuilder};
use std::borrow::Cow;

/// Implementation of the SPARQL `regex` function (binary version).
#[derive(Debug)]
pub struct RegexSparqlOp {}

impl Default for RegexSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Regex);

    /// Creates a new [RegexSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for RegexSparqlOp {
    type Args<TEncoding: TermEncoding> = BinaryOrTernaryArgs<TEncoding>;

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
            BinaryOrTernaryArgs::Binary(BinaryArgs(lhs, rhs)) => dispatch_binary_typed_value(
                &lhs,
                &rhs,
                |lhs_value, rhs_value| {
                    let TypedValueRef::SimpleLiteral(pattern) = rhs_value else {
                        return ThinError::expected();
                    };

                    let regex = compile_pattern(pattern.value, None)?;
                    match lhs_value {
                        TypedValueRef::SimpleLiteral(value) => Ok(TypedValueRef::BooleanLiteral(
                            regex.is_match(value.value).into(),
                        )),
                        TypedValueRef::LanguageStringLiteral(value) => Ok(
                            TypedValueRef::BooleanLiteral(regex.is_match(value.value).into()),
                        ),
                        _ => ThinError::expected(),
                    }
                },
                |_, _| ThinError::expected(),
            ),
            BinaryOrTernaryArgs::Ternary(_) => todo!("Dispatch")
        }
    }
}

fn compile_pattern(pattern: &str, flags: Option<&str>) -> ThinResult<Regex> {
    const REGEX_SIZE_LIMIT: usize = 1_000_000;

    let mut pattern = Cow::Borrowed(pattern);
    let flags = flags.unwrap_or_default();
    if flags.contains('q') {
        pattern = regex::escape(&pattern).into();
    }
    let mut regex_builder = RegexBuilder::new(&pattern);
    regex_builder.size_limit(REGEX_SIZE_LIMIT);
    for flag in flags.chars() {
        match flag {
            's' => {
                regex_builder.dot_matches_new_line(true);
            }
            'm' => {
                regex_builder.multi_line(true);
            }
            'i' => {
                regex_builder.case_insensitive(true);
            }
            'x' => {
                regex_builder.ignore_whitespace(true);
            }
            'q' => (),                         // Already supported
            _ => return ThinError::expected(), // invalid option
        }
    }
    regex_builder.build().map_err(|_| ThinError::Expected)
}
