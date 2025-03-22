use crate::{RdfOpResult, TermRef, RdfValueRef};
use oxrdf::BlankNodeRef;

impl<'data> RdfValueRef<'data> for BlankNodeRef<'data> {
    fn from_term(term: TermRef<'data>) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        match term {
            TermRef::BlankNode(inner) => Ok(inner),
            _ => Err(()),
        }
    }
}
