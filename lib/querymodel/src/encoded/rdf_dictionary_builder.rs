use crate::encoded::EncDictKey;
use crate::AResult;
use datafusion::arrow::array::ArrayRef;

pub struct EncodedStringBuilder {}

impl EncodedStringBuilder {
    pub fn new() -> Self {
        // TODO: some callback to decode stuff.
        Self {}
    }

    pub fn append_value(&mut self, key: &EncDictKey, value: &str) -> AResult<()> {
        // TODO:

        Ok(())
    }

    pub fn finish(self) -> ArrayRef {
        unimplemented!()
    }
}
