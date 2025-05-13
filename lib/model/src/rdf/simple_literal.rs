#[derive(Clone, PartialEq, Eq, Debug, PartialOrd, Ord, Hash)]
pub struct SimpleLiteral {
    pub value: String,
}

impl SimpleLiteral {
    pub fn as_ref(&self) -> SimpleLiteralRef<'_> {
        SimpleLiteralRef { value: &self.value }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct SimpleLiteralRef<'value> {
    pub value: &'value str,
}

impl<'value> SimpleLiteralRef<'value> {
    pub fn new(value: &'value str) -> Self {
        Self { value }
    }

    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    pub fn into_owned(self) -> SimpleLiteral {
        SimpleLiteral {
            value: self.value.to_owned(),
        }
    }
}
