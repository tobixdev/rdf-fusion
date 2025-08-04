use crate::object_id::{ObjectIdArray, ObjectIdEncoding};
use crate::TermEncoding;
use datafusion::arrow::array::FixedSizeBinaryBuilder;
use rdf_fusion_common::{AResult, ObjectId};
use std::sync::Arc;

/// Provides a convenient API for building arrays of RDF terms with the [ObjectIdEncoding]. The
/// documentation of the encoding provides additional information.
pub struct ObjectIdArrayBuilder {
    /// The mapping that is used for obtaining object ids.
    encoding: ObjectIdEncoding,
    /// The underlying [FixedSizeBinaryBuilder].
    builder: FixedSizeBinaryBuilder,
}

impl ObjectIdArrayBuilder {
    /// Create a [ObjectIdArrayBuilder] with the given `capacity`.
    pub fn new(encoding: ObjectIdEncoding) -> Self {
        let builder = FixedSizeBinaryBuilder::new(encoding.object_id_size() as i32);
        Self { encoding, builder }
    }

    /// Appends a null value to the array.
    pub fn append_null(&mut self) {
        self.builder.append_null()
    }

    /// Appends an object id.
    pub fn append_object_id_bytes(&mut self, term: &[u8]) -> AResult<()> {
        self.builder.append_value(term)
    }

    /// Appends an object id.
    pub fn append_object_id(&mut self, term: ObjectId) -> AResult<()> {
        self.builder.append_value(term)
    }

    /// Appends an object id.
    pub fn append_object_id_opt(&mut self, term: Option<ObjectId>) -> AResult<()> {
        match term {
            None => {
                self.builder.append_null();
                Ok(())
            }
            Some(term) => self.builder.append_value(term),
        }
    }

    #[allow(clippy::expect_used, reason = "Programming error")]
    pub fn finish(mut self) -> ObjectIdArray {
        let encoding = self.encoding;
        let array = Arc::new(self.builder.finish());
        encoding.try_new_array(array).expect("Builder fixed")
    }
}
