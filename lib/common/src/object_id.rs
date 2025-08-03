#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct ObjectId(Box<[u8]>);

impl ObjectId {
    pub const fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8]> for ObjectId {
    fn from(value: &[u8]) -> Self {
        Self(value.into())
    }
}

impl AsRef<[u8]> for ObjectId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Into<Vec<u8>> for ObjectId {
    fn into(self) -> Vec<u8> {
        self.0.to_vec()
    }
}
