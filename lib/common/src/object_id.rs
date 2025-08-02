use thiserror::Error;

#[derive(Eq, PartialEq, Debug, Clone, Copy, Hash)]
pub struct ObjectId([u8; ObjectId::SIZE]);

impl ObjectId {
    pub const SIZE: usize = 20;
    pub const SIZE_I32: i32 = 20;

    pub const fn from_u64(value: u64) -> Self {
        let bytes = value.to_be_bytes();
        let mut buffer = [0u8; ObjectId::SIZE];

        // Manual byte copy — const-compatible
        buffer[4] = bytes[0];
        buffer[5] = bytes[1];
        buffer[6] = bytes[2];
        buffer[7] = bytes[3];
        buffer[8] = bytes[4];
        buffer[9] = bytes[5];
        buffer[10] = bytes[6];
        buffer[11] = bytes[7];

        Self(buffer)
    }

    pub const fn into_bytes(self) -> [u8; ObjectId::SIZE] {
        self.0
    }
}

impl From<[u8; ObjectId::SIZE]> for ObjectId {
    fn from(value: [u8; ObjectId::SIZE]) -> Self {
        Self(value)
    }
}

impl From<u64> for ObjectId {
    fn from(value: u64) -> Self {
        Self::from_u64(value)
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

#[derive(Debug, Error)]
#[error("Invalid ObjectId size")]
pub struct InvalidObjectIdSizeError;

impl TryFrom<&[u8]> for ObjectId {
    type Error = InvalidObjectIdSizeError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        TryInto::<[u8; ObjectId::SIZE]>::try_into(value)
            .map(ObjectId::from)
            .map_err(|_| InvalidObjectIdSizeError)
    }
}
