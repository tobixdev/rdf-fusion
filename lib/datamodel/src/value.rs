use crate::{RdfOpResult, TermRef};

pub trait RdfValueRef<'data>: Copy {
    fn from_term(term: TermRef<'data>) -> RdfOpResult<Self>
    where
        Self: Sized;
}
