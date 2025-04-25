use crate::{TermRef, ThinResult};

pub trait RdfValueRef<'data>: Copy {
    fn from_term(term: TermRef<'data>) -> ThinResult<Self>
    where
        Self: Sized;
}
