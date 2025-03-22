use crate::{RdfOpResult, ScalarBinaryRdfOp};
use datamodel::{Boolean, SimpleLiteralRef};

#[derive(Debug)]
pub struct LangMatchesRdfOp {}

impl LangMatchesRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for LangMatchesRdfOp {
    type ArgLhs<'lhs> = SimpleLiteralRef<'lhs>;
    type ArgRhs<'lhs> = SimpleLiteralRef<'lhs>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        language_tag: Self::ArgLhs<'data>,
        language_range: Self::ArgRhs<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        let matches = if &*language_range.value == "*" {
            !language_tag.value.is_empty()
        } else {
            !ZipLongest::new(
                language_range.value.split('-'),
                language_tag.value.split('-'),
            )
            .any(|parts| match parts {
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
