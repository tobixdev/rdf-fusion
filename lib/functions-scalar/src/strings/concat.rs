use crate::{NArySparqlOp, SparqlOp, ThinResult};
use rdf_fusion_model::{OwnedStringLiteral, StringLiteralRef};

#[derive(Debug)]
pub struct ConcatSparqlOp;

impl Default for ConcatSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl ConcatSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for ConcatSparqlOp {}

impl NArySparqlOp for ConcatSparqlOp {
    type Args<'data> = StringLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, args: &[Self::Args<'data>]) -> ThinResult<Self::Result<'data>> {
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
            result += arg.0;
        }

        Ok(OwnedStringLiteral(
            result,
            language.flatten().map(ToOwned::to_owned),
        ))
    }
}
