use crate::{InternalTermRef, RdfValueRef, ThinError, ThinResult};
use oxrdf::NamedNodeRef;

impl<'data> RdfValueRef<'data> for NamedNodeRef<'data> {
    fn from_term(term: InternalTermRef<'data>) -> ThinResult<Self>
    where
        Self: Sized,
    {
        match term {
            InternalTermRef::NamedNode(inner) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}
