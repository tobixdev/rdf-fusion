use crate::{ScalarBinaryRdfOp, ScalarTernaryRdfOp, ThinResult};
use datamodel::{Boolean, SimpleLiteralRef, StringLiteralRef, ThinError};
use regex::{Regex, RegexBuilder};
use std::borrow::Cow;

#[derive(Debug)]
pub struct RegexRdfOp;

// TODO: Support pre-compiled regex if not a variable

impl Default for RegexRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for RegexRdfOp {
    type ArgLhs<'data> = StringLiteralRef<'data>;
    type ArgRhs<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        let regex = compile_pattern(rhs.value, None)?;
        Ok(regex.is_match(lhs.0).into())
    }
}

impl ScalarTernaryRdfOp for RegexRdfOp {
    type Arg0<'data> = StringLiteralRef<'data>;
    type Arg1<'data> = SimpleLiteralRef<'data>;
    type Arg2<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        arg0: Self::Arg0<'data>,
        arg1: Self::Arg1<'data>,
        arg2: Self::Arg2<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        let regex = compile_pattern(arg1.value, Some(arg2.value))?;
        Ok(regex.is_match(arg0.0).into())
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
    regex_builder.build().map_err(|_| ThinError::Expected)
}
