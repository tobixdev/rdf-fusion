use crate::{
    Boolean, Duration, LanguageStringRef, Numeric, RdfOpResult, RdfValueRef, SimpleLiteralRef, TypedLiteralRef,
};
use oxrdf::{BlankNodeRef, NamedNodeRef};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TermRef<'value> {
    NamedNode(NamedNodeRef<'value>),
    BlankNode(BlankNodeRef<'value>),
    Boolean(Boolean),
    Numeric(Numeric),
    SimpleLiteral(SimpleLiteralRef<'value>),
    LanguageString(LanguageStringRef<'value>),
    Duration(Duration),
    TypedLiteral(TypedLiteralRef<'value>),
}

impl<'data> RdfValueRef<'data> for TermRef<'data> {
    fn from_term(term: TermRef<'data>) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        Ok(term)
    }
}
