use crate::TermEncoding;
use crate::object_id::{ObjectIdArray, ObjectIdEncoding};
use datafusion::arrow::array::FixedSizeBinaryBuilder;
use rdf_fusion_common::{AResult, ObjectId};
use rdf_fusion_model::TermRef;
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
        Self {
            encoding,
            builder: FixedSizeBinaryBuilder::new(ObjectId::SIZE as i32),
        }
    }

    /// Appends a null value to the array.
    pub fn append_null(&mut self) {
        self.builder.append_null()
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

    /// Appends an arbitrary RDF term to the array. The corresponding object id is obtained by
    /// consulting the mapping.
    pub fn append_term(&mut self, term: TermRef<'_>) -> AResult<()> {
        let value = self.encoding.mapping().encode(term);
        self.append_object_id(value)
    }

    #[allow(clippy::expect_used, reason = "Programming error")]
    pub fn finish(mut self) -> ObjectIdArray {
        let encoding = self.encoding;
        let array = Arc::new(self.builder.finish());
        encoding.try_new_array(array).expect("Builder fixed")
    }
}
