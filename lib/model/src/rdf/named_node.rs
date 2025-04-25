use crate::{RdfValueRef, TermRef, ThinError, ThinResult};
use oxrdf::NamedNodeRef;

impl<'data> RdfValueRef<'data> for NamedNodeRef<'data> {
    fn from_term(term: TermRef<'data>) -> ThinResult<Self>
    where
        Self: Sized,
    {
        match term {
            TermRef::NamedNode(inner) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}
