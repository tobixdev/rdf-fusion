use crate::{RdfOpError, RdfOpResult, RdfValueRef, TermRef};
use oxrdf::NamedNodeRef;

impl<'data> RdfValueRef<'data> for NamedNodeRef<'data> {
    fn from_term(term: TermRef<'data>) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        match term {
            TermRef::NamedNode(inner) => Ok(inner),
            _ => Err(RdfOpError),
        }
    }
}
