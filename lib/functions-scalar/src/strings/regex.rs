use crate::{RdfOpResult, ScalarBinaryRdfOp, ScalarTernaryRdfOp};
use datamodel::{Boolean, SimpleLiteralRef, StringLiteral};
use regex::{Regex, RegexBuilder};
use std::borrow::Cow;

#[derive(Debug)]
pub struct RegexRdfOp {}

// TODO: Support pre-compiled regex if not a variable

impl RegexRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for RegexRdfOp {
    type ArgLhs<'data> = StringLiteral<'data>;
    type ArgRhs<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        text: Self::ArgLhs<'data>,
        pattern: Self::ArgRhs<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        let regex = compile_pattern(&pattern.value, None).ok_or(())?;
        Ok(regex.is_match(&text.0).into())
    }
}

impl ScalarTernaryRdfOp for RegexRdfOp {
    type Arg0<'data> = StringLiteral<'data>;
    type Arg1<'data> = SimpleLiteralRef<'data>;
    type Arg2<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        text: Self::Arg0<'data>,
        pattern: Self::Arg1<'data>,
        flags: Self::Arg2<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        let regex = compile_pattern(&pattern.value, Some(flags.value)).ok_or(())?;
        Ok(regex.is_match(&text.0).into())
    }
}

pub(super) fn compile_pattern(pattern: &str, flags: Option<&str>) -> Option<Regex> {
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
            'q' => (),        // Already supported
            _ => return None, // invalid option
        }
    }
    regex_builder.build().ok()
}
