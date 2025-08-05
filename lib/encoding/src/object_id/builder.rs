use crate::TermEncoding;
use crate::object_id::{ObjectIdArray, ObjectIdEncoding};
use datafusion::arrow::array::UInt32Builder;
use rdf_fusion_common::ObjectId;
use std::sync::Arc;

/// Provides a convenient API for building arrays of RDF terms with the [ObjectIdEncoding]. The
/// documentation of the encoding provides additional information.
pub struct ObjectIdArrayBuilder {
    /// The mapping that is used for obtaining object ids.
    encoding: ObjectIdEncoding,
    /// The underlying [UInt32Builder].
    builder: UInt32Builder,
}

impl ObjectIdArrayBuilder {
    /// Create a [ObjectIdArrayBuilder] with the given `capacity`.
    pub fn new(encoding: ObjectIdEncoding) -> Self {
        let builder = UInt32Builder::new();
        Self { encoding, builder }
    }

    /// Appends a null value to the array.
    pub fn append_null(&mut self) {
        self.builder.append_null()
    }

    /// Appends an object id.
    pub fn append_object_id(&mut self, term: ObjectId) {
        self.builder.append_value(term.0)
    }

    /// Appends an object id.
    pub fn append_object_id_opt(&mut self, term: Option<ObjectId>) {
        self.builder.append_option(term.map(|t| t.0))
    }

    #[allow(clippy::expect_used, reason = "Programming error")]
    pub fn finish(mut self) -> ObjectIdArray {
        let encoding = self.encoding;
        let array = Arc::new(self.builder.finish());
        encoding.try_new_array(array).expect("Builder fixed")
    }
}
