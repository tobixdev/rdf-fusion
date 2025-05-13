use crate::{BinarySparqlOp, SparqlOp, ThinResult};
use rdf_fusion_model::{Boolean, SimpleLiteralRef};

#[derive(Debug)]
pub struct LangMatchesSparqlOp;

impl Default for LangMatchesSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl LangMatchesSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for LangMatchesSparqlOp {}

impl BinarySparqlOp for LangMatchesSparqlOp {
    type ArgLhs<'lhs> = SimpleLiteralRef<'lhs>;
    type ArgRhs<'lhs> = SimpleLiteralRef<'lhs>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        let matches = if rhs.value == "*" {
            !lhs.value.is_empty()
        } else {
            !ZipLongest::new(rhs.value.split('-'), lhs.value.split('-')).any(|parts| match parts {
                (Some(range_subtag), Some(language_subtag)) => {
                    !range_subtag.eq_ignore_ascii_case(language_subtag)
                }
                (Some(_), None) => true,
                (None, _) => false,
            })
        };
        Ok(matches.into())
    }
}

struct ZipLongest<T1, T2, I1: Iterator<Item = T1>, I2: Iterator<Item = T2>> {
    a: I1,
    b: I2,
}

impl<T1, T2, I1: Iterator<Item = T1>, I2: Iterator<Item = T2>> ZipLongest<T1, T2, I1, I2> {
    fn new(a: I1, b: I2) -> Self {
        Self { a, b }
    }
}

impl<T1, T2, I1: Iterator<Item = T1>, I2: Iterator<Item = T2>> Iterator
    for ZipLongest<T1, T2, I1, I2>
{
    type Item = (Option<T1>, Option<T2>);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.a.next(), self.b.next()) {
            (None, None) => None,
            r => Some(r),
        }
    }
}
