use crate::scalar::dispatch::{
    dispatch_binary_typed_value, dispatch_ternary_typed_value,
};
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{BinaryArgs, BinaryOrTernaryArgs, ScalarSparqlOp, TernaryArgs};
use datafusion::logical_expr::Volatility;
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{SimpleLiteralRef, ThinError, ThinResult, TypedValueRef};
use regex::{Regex, RegexBuilder};
use std::borrow::Cow;

/// Implementation of the SPARQL `regex` function (binary version).
#[derive(Debug)]
pub struct RegexSparqlOp;

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

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<TypedValueEncoding>>>> {
        Some(create_typed_value_sparql_op_impl(|args| match args {
            BinaryOrTernaryArgs::Binary(BinaryArgs(lhs, rhs)) => {
                dispatch_binary_typed_value(
                    &lhs,
                    &rhs,
                    |lhs_value, rhs_value| {
                        let TypedValueRef::SimpleLiteral(pattern) = rhs_value else {
                            return ThinError::expected();
                        };

                        let regex = compile_pattern(pattern.value, None)?;
                        match lhs_value {
                            TypedValueRef::SimpleLiteral(value) => {
                                Ok(TypedValueRef::BooleanLiteral(
                                    regex.is_match(value.value).into(),
                                ))
                            }
                            TypedValueRef::LanguageStringLiteral(value) => {
                                Ok(TypedValueRef::BooleanLiteral(
                                    regex.is_match(value.value).into(),
                                ))
                            }
                            _ => ThinError::expected(),
                        }
                    },
                    |_, _| ThinError::expected(),
                )
            }
            BinaryOrTernaryArgs::Ternary(TernaryArgs(arg0, arg1, arg2)) => {
                dispatch_ternary_typed_value(
                    &arg0,
                    &arg1,
                    &arg2,
                    |arg0, arg1, arg2| {
                        let arg1 = SimpleLiteralRef::try_from(arg1)?;
                        let arg2 = SimpleLiteralRef::try_from(arg2)?;

                        let regex = compile_pattern(arg1.value, Some(arg2.value))?;
                        match arg0 {
                            TypedValueRef::SimpleLiteral(value) => {
                                Ok(TypedValueRef::BooleanLiteral(
                                    regex.is_match(value.value).into(),
                                ))
                            }
                            TypedValueRef::LanguageStringLiteral(value) => {
                                Ok(TypedValueRef::BooleanLiteral(
                                    regex.is_match(value.value).into(),
                                ))
                            }
                            _ => ThinError::expected(),
                        }
                    },
                    |_, _, _| ThinError::expected(),
                )
            }
        }))
    }
}

pub(super) fn compile_pattern(pattern: &str, flags: Option<&str>) -> ThinResult<Regex> {
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
    regex_builder.build().map_err(|_| ThinError::ExpectedError)
}
