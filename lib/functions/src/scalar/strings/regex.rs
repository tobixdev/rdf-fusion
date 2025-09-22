use crate::scalar::dispatch::{
    dispatch_binary_typed_value, dispatch_ternary_typed_value,
};
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{SimpleLiteralRef, ThinError, ThinResult, TypedValueRef};
use regex::{Regex, RegexBuilder};
use std::borrow::Cow;

/// Implementation of the SPARQL `regex` function (binary version).
#[derive(Debug, Hash, PartialEq, Eq)]
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
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn details(&self) -> ScalarSparqlOpDetails {
        ScalarSparqlOpDetails::default_with_arity(SparqlOpArity::OneOf(vec![
            SparqlOpArity::Fixed(2),
            SparqlOpArity::Fixed(3),
        ]))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            match args.args.len() {
                2 => dispatch_binary_typed_value(
                    &args.args[0],
                    &args.args[1],
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
                ),
                3 => dispatch_ternary_typed_value(
                    &args.args[0],
                    &args.args[1],
                    &args.args[2],
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
                ),
                _ => unreachable!("Invalid number of arguments"),
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
