#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct ObjectIdRef<'a>(&'a [u8]);

impl ObjectIdRef<'_> {
    pub const fn as_bytes(&self) -> &[u8] {
        self.0
    }
}

impl<'a> From<&'a [u8]> for ObjectIdRef<'a> {
    fn from(value: &'a [u8]) -> Self {
        Self(value)
    }
}

impl AsRef<[u8]> for ObjectIdRef<'_> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}
