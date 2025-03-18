use crate::strings::regex::compile_pattern;
use crate::{RdfOpResult, ScalarQuaternaryRdfOp, ScalarTernaryRdfOp};
use datamodel::{OwnedStringLiteral, SimpleLiteralRef, StringLiteral};
use std::borrow::Cow;

#[derive(Debug)]
pub struct ReplaceRdfOp {}

// TODO: Support pre-compiled regex if not a variable

impl ReplaceRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarTernaryRdfOp for ReplaceRdfOp {
    type Arg0<'data> = StringLiteral<'data>;
    type Arg1<'data> = SimpleLiteralRef<'data>;
    type Arg2<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(
        &self,
        text: Self::Arg0<'data>,
        pattern: Self::Arg1<'data>,
        replacement: Self::Arg2<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        let regex = compile_pattern(&pattern.value, None).ok_or(())?;

        let result = match regex.replace_all(&text.0, replacement.value) {
            Cow::Owned(replaced) => replaced,
            Cow::Borrowed(_) => text.0.to_string(),
        };

        Ok(OwnedStringLiteral::new(result, text.1.map(String::from)))
    }
}

impl ScalarQuaternaryRdfOp for ReplaceRdfOp {
    type Arg0<'data> = StringLiteral<'data>;
    type Arg1<'data> = SimpleLiteralRef<'data>;
    type Arg2<'data> = SimpleLiteralRef<'data>;
    type Arg3<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(
        &self,
        text: Self::Arg0<'data>,
        pattern: Self::Arg1<'data>,
        replacement: Self::Arg2<'data>,
        flags: Self::Arg3<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        let regex = compile_pattern(&pattern.value, Some(flags.value)).ok_or(())?;

        let result = match regex.replace_all(&text.0, replacement.value) {
            Cow::Owned(replaced) => replaced,
            Cow::Borrowed(_) => text.0.to_string(),
        };

        Ok(OwnedStringLiteral::new(result, text.1.map(String::from)))
    }
}
