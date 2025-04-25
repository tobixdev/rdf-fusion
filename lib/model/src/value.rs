use crate::{InternalTermRef, ThinResult};

pub trait RdfValueRef<'data>: Copy {
    fn from_term(term: InternalTermRef<'data>) -> ThinResult<Self>
    where
        Self: Sized;
}
