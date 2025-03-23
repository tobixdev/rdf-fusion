use crate::{Boolean, DayTimeDuration, Duration, LanguageStringRef, Numeric, RdfOpResult, RdfValueRef, SimpleLiteralRef, TypedLiteralRef, YearMonthDuration};
use oxrdf::{BlankNodeRef, NamedNodeRef};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TermRef<'value> {
    NamedNode(NamedNodeRef<'value>),
    BlankNode(BlankNodeRef<'value>),
    BooleanLiteral(Boolean),
    NumericLiteral(Numeric),
    SimpleLiteral(SimpleLiteralRef<'value>),
    LanguageStringLiteral(LanguageStringRef<'value>),
    DurationLiteral(Duration),
    YearMonthDurationLiteral(YearMonthDuration),
    DayTimeDurationLiteral(DayTimeDuration),
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
