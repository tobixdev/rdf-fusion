use crate::strings::regex::compile_pattern;
use crate::{QuaternarySparqlOp, SparqlOp, TernarySparqlOp, ThinResult};
use graphfusion_model::{OwnedStringLiteral, SimpleLiteralRef, StringLiteralRef};
use std::borrow::Cow;

#[derive(Debug)]
pub struct ReplaceSparqlOp;

// TODO: Support pre-compiled regex if not a variable

impl Default for ReplaceSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplaceSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for ReplaceSparqlOp {}

impl TernarySparqlOp for ReplaceSparqlOp {
    type Arg0<'data> = StringLiteralRef<'data>;
    type Arg1<'data> = SimpleLiteralRef<'data>;
    type Arg2<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(
        &self,
        arg0: Self::Arg0<'data>,
        arg1: Self::Arg1<'data>,
        arg2: Self::Arg2<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        let regex = compile_pattern(arg1.value, None)?;

        let result = match regex.replace_all(arg0.0, arg2.value) {
            Cow::Owned(replaced) => replaced,
            Cow::Borrowed(_) => arg0.0.to_owned(),
        };

        Ok(OwnedStringLiteral::new(result, arg0.1.map(String::from)))
    }
}

impl QuaternarySparqlOp for ReplaceSparqlOp {
    type Arg0<'data> = StringLiteralRef<'data>;
    type Arg1<'data> = SimpleLiteralRef<'data>;
    type Arg2<'data> = SimpleLiteralRef<'data>;
    type Arg3<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(
        &self,
        arg0: Self::Arg0<'data>,
        arg1: Self::Arg1<'data>,
        arg2: Self::Arg2<'data>,
        arg3: Self::Arg3<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        let regex = compile_pattern(arg1.value, Some(arg3.value))?;

        let result = match regex.replace_all(arg0.0, arg2.value) {
            Cow::Owned(replaced) => replaced,
            Cow::Borrowed(_) => arg0.0.to_owned(),
        };

        Ok(OwnedStringLiteral::new(result, arg0.1.map(String::from)))
    }
}
