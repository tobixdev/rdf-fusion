use crate::{RdfOpResult, TermRef, RdfValueRef};

#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct SimpleLiteralRef<'value> {
    pub value: &'value str,
}

impl<'value> SimpleLiteralRef<'value> {
    pub fn new(value: &'value str) -> Self {
        Self { value }
    }

    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }
}

impl<'data> RdfValueRef<'data> for SimpleLiteralRef<'data> {
    fn from_term(term: TermRef<'data>) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        match term {
            TermRef::SimpleLiteral(inner) => Ok(inner),
            _ => Err(()),
        }
    }
}
