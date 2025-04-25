use crate::{ScalarUnaryRdfOp, ThinResult};
use datamodel::{OwnedStringLiteral, StringLiteralRef};

#[derive(Debug)]
pub struct EncodeForUriRdfOp;

impl Default for EncodeForUriRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl EncodeForUriRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for EncodeForUriRdfOp {
    type Arg<'data> = StringLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        // Based on oxigraph/lib/spareval/src/eval.rs
        // Maybe we can use a library in the future?
        let mut result = Vec::with_capacity(value.0.len());
        for c in value.0.bytes() {
            match c {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    result.push(c)
                }
                _ => {
                    result.push(b'%');
                    let high = c / 16;
                    let low = c % 16;
                    result.push(if high < 10 {
                        b'0' + high
                    } else {
                        b'A' + (high - 10)
                    });
                    result.push(if low < 10 {
                        b'0' + low
                    } else {
                        b'A' + (low - 10)
                    });
                }
            }
        }

        let result = String::from_utf8(result)?;
        Ok(OwnedStringLiteral::new(result, None))
    }
}
