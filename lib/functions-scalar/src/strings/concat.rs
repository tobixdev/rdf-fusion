use crate::{RdfOpResult, ScalarNAryRdfOp};
use datamodel::{OwnedStringLiteral, StringLiteralRef};

#[derive(Debug)]
pub struct ConcatRdfOp {}

impl ConcatRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarNAryRdfOp for ConcatRdfOp {
    type Args<'data> = StringLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, args: &[Self::Args<'data>]) -> RdfOpResult<Self::Result<'data>> {
        let mut result = String::default();
        let mut language = None;

        for arg in args {
            if let Some(lang) = &language {
                if *lang != arg.1 {
                    language = Some(None)
                }
            } else {
                language = Some(arg.1)
            }
            result += &arg.0;
        }

        Ok(OwnedStringLiteral(
            result,
            language.flatten().map(ToOwned::to_owned),
        ))
    }
}
