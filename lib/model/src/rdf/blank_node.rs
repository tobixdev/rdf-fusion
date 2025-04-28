use crate::{InternalTermRef, RdfValueRef, ThinError, ThinResult};
use oxrdf::BlankNodeRef;

impl<'data> RdfValueRef<'data> for BlankNodeRef<'data> {
    fn from_term(term: InternalTermRef<'data>) -> ThinResult<Self>
    where
        Self: Sized,
    {
        match term {
            InternalTermRef::BlankNode(inner) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}
