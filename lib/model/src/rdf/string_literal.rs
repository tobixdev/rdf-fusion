use crate::{RdfValueRef, TermRef, ThinError, ThinResult};
use std::cmp::Ordering;

/// https://www.w3.org/TR/sparql11-query/#func-string
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct StringLiteralRef<'value>(pub &'value str, pub Option<&'value str>);

impl StringLiteralRef<'_> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.chars().count()
    }
}

impl PartialOrd for StringLiteralRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StringLiteralRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(other.0)
    }
}

// TODO: This should only be a temporary solution once the results can write into the arrays.

/// https://www.w3.org/TR/sparql11-query/#func-string
#[derive(PartialEq, Eq, Debug)]
pub struct OwnedStringLiteral(pub String, pub Option<String>);

impl OwnedStringLiteral {
    pub fn new(value: String, language: Option<String>) -> OwnedStringLiteral {
        OwnedStringLiteral(value, language)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.chars().count()
    }
}

pub struct CompatibleStringArgs<'data> {
    pub lhs: &'data str,
    pub rhs: &'data str,
    pub language: Option<&'data str>,
}

impl<'data> CompatibleStringArgs<'data> {
    /// Checks whether two [StringLiteralRef] are compatible and if they are return a new
    /// [CompatibleStringArgs].
    ///
    /// https://www.w3.org/TR/2013/REC-sparql11-query-20130321/#func-arg-compatibility
    pub fn try_from(
        lhs: StringLiteralRef<'data>,
        rhs: StringLiteralRef<'data>,
    ) -> ThinResult<CompatibleStringArgs<'data>> {
        let is_compatible = rhs.1.is_none() || lhs.1 == rhs.1;

        if !is_compatible {
            return ThinError::expected();
        }

        Ok(CompatibleStringArgs {
            lhs: lhs.0,
            rhs: rhs.0,
            language: lhs.1,
        })
    }
}

impl<'data> RdfValueRef<'data> for StringLiteralRef<'data> {
    fn from_term(term: TermRef<'data>) -> ThinResult<Self>
    where
        Self: Sized,
    {
        match term {
            TermRef::SimpleLiteral(inner) => Ok(StringLiteralRef(inner.value, None)),
            TermRef::LanguageStringLiteral(inner) => {
                Ok(StringLiteralRef(inner.value, Some(inner.language)))
            }
            _ => ThinError::expected(),
        }
    }
}
