use crate::{RdfOpResult, TermRef, RdfValueRef};
use std::cmp::Ordering;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct LanguageStringRef<'value> {
    pub value: &'value str,
    pub language: &'value str,
}

impl LanguageStringRef<'_> {
    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }
}

impl PartialOrd for LanguageStringRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.value.partial_cmp(other.value)
    }
}

impl Ord for LanguageStringRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).expect("Ordering is total")
    }
}

impl<'data> RdfValueRef<'data> for LanguageStringRef<'data> {
    fn from_term(term: TermRef<'data>) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        match term {
            TermRef::LanguageString(inner) => Ok(inner),
            _ => Err(()),
        }
    }
}
