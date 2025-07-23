use crate::object_id::{ObjectIdArray, ObjectIdEncoding};
use crate::TermEncoding;
use datafusion::arrow::array::UInt64Builder;
use rdf_fusion_model::TermRef;
use std::sync::Arc;

/// Provides a convenient API for building arrays of RDF terms with the [ObjectIdEncoding]. The
/// documentation of the encoding provides additional information.
pub struct ObjectIdArrayBuilder {
    /// The mapping that is used for obtaining object ids.
    encoding: ObjectIdEncoding,
    /// The underlying [UInt64Builder].
    builder: UInt64Builder,
}

impl ObjectIdArrayBuilder {
    /// Create a [ObjectIdArrayBuilder] with the given `capacity`.
    pub fn new(encoding: ObjectIdEncoding) -> Self {
        Self {
            encoding,
            builder: UInt64Builder::new(),
        }
    }

    /// Appends a null value to the array.
    pub fn append_null(&mut self) {
        self.builder.append_value(0);
    }

    /// Appends an arbitrary RDF term to the array. The corresponding object id is obtained by
    /// consulting the mapping.
    pub fn append_term(&mut self, term: TermRef<'_>) {
        let value = self.encoding.mapping().encode(term);
        self.builder.append_value(value);
    }

    #[allow(clippy::expect_used, reason = "Programming error")]
    pub fn finish(mut self) -> ObjectIdArray {
        let encoding = self.encoding;
        let array = Arc::new(self.builder.finish());
        encoding.try_new_array(array).expect("Builder fixed")
    }
}
