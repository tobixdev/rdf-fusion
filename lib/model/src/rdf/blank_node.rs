use crate::{RdfValueRef, TermRef, ThinError, ThinResult};
use oxrdf::BlankNodeRef;

impl<'data> RdfValueRef<'data> for BlankNodeRef<'data> {
    fn from_term(term: TermRef<'data>) -> ThinResult<Self>
    where
        Self: Sized,
    {
        match term {
            TermRef::BlankNode(inner) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}
