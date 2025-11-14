use crate::object_id::{ObjectIdEncoding, ObjectIdEncodingRef, ObjectIdMapping};
use crate::plain_term::{PlainTermEncoding, PlainTermEncodingRef};
use crate::sortable_term::SortableTermEncoding;
use crate::typed_value::{TypedValueEncoding, TypedValueEncodingRef};
use crate::{EncodingName, TermEncoding};
use datafusion::arrow::datatypes::DataType;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Holds a configuration instance for each RDF Fusion encoding.
///
/// This is an instance (as opposed to a type) as some encodings can be configured. At least
/// this is planned for the future. For each RDF Fusion instance, the encodings are fixed once it
/// is created.
///
/// # Equality
///
/// The equality and hashing functions check for pointer equality of the underlying encodings.
#[derive(Debug, Clone)]
pub struct RdfFusionEncodings {
    /// The [PlainTermEncoding] configuration.
    plain_term: PlainTermEncodingRef,
    /// The [TypedValueEncoding] configuration.
    typed_value: TypedValueEncodingRef,
    /// The [ObjectIdEncoding] configuration.
    object_id: Option<Arc<ObjectIdEncoding>>,
    /// The [SortableTermEncoding] configuration.
    sortable_term: Arc<SortableTermEncoding>,
}

impl RdfFusionEncodings {
    /// Creates a new [RdfFusionEncodings].
    pub fn new(
        plain_term: PlainTermEncodingRef,
        typed_value: TypedValueEncodingRef,
        object_id: Option<ObjectIdEncodingRef>,
        sortable_term: SortableTermEncoding,
    ) -> Self {
        Self {
            plain_term,
            typed_value,
            object_id,
            sortable_term: Arc::new(sortable_term),
        }
    }

    /// Provides a reference to the used [PlainTermEncoding].
    pub fn plain_term(&self) -> &PlainTermEncoding {
        &self.plain_term
    }

    /// Provides a reference to the used [TypedValueEncoding].
    pub fn typed_value(&self) -> &TypedValueEncoding {
        &self.typed_value
    }

    /// Provides a reference to the used [ObjectIdEncoding].
    pub fn object_id(&self) -> Option<&ObjectIdEncoding> {
        self.object_id.as_ref().map(AsRef::as_ref)
    }

    /// Provides a reference to the used [ObjectIdEncoding].
    pub fn object_id_mapping(&self) -> Option<&dyn ObjectIdMapping> {
        self.object_id.as_ref().map(|enc| enc.mapping().as_ref())
    }

    /// Provides a reference to the used [SortableTermEncoding].
    pub fn sortable_term(&self) -> &SortableTermEncoding {
        &self.sortable_term
    }

    /// Returns a vector of [EncodingName] for the given `names`.
    ///
    /// If some encodings are not defined in this RDF Fusion instance (e.g., no object ID encoding),
    /// the corresponding [EncodingName] is ignored.
    pub fn get_data_types(&self, names: &[EncodingName]) -> Vec<DataType> {
        let mut result = Vec::new();

        if names.contains(&EncodingName::PlainTerm) {
            result.push(self.plain_term.data_type().clone());
        }

        if names.contains(&EncodingName::TypedValue) {
            result.push(self.typed_value.data_type().clone());
        }

        if let Some(object_id) = self.object_id.as_ref()
            && names.contains(&EncodingName::ObjectId)
        {
            result.push(object_id.as_ref().data_type().clone());
        }

        if names.contains(&EncodingName::Sortable) {
            result.push(self.sortable_term.data_type().clone());
        }

        result
    }

    /// Tries to obtain an [EncodingName] from a [DataType]. As we currently only support built-in
    /// encodings this mapping is unique.
    ///
    /// In the future we might use a field here such that we can access metadata information.
    pub fn try_get_encoding_name(&self, data_type: &DataType) -> Option<EncodingName> {
        if data_type == PlainTermEncoding.data_type() {
            return Some(EncodingName::PlainTerm);
        }

        if data_type == self.typed_value.data_type() {
            return Some(EncodingName::TypedValue);
        }

        if let Some(object_id) = self.object_id.as_ref()
            && data_type == object_id.data_type()
        {
            return Some(EncodingName::ObjectId);
        }

        if data_type == self.sortable_term.data_type() {
            return Some(EncodingName::Sortable);
        }

        None
    }
}

impl PartialEq for RdfFusionEncodings {
    fn eq(&self, other: &Self) -> bool {
        let object_id_equal = match (&self.object_id, &other.object_id) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        };

        object_id_equal
            && Arc::ptr_eq(&self.plain_term, &other.plain_term)
            && Arc::ptr_eq(&self.typed_value, &other.typed_value)
            && Arc::ptr_eq(&self.sortable_term, &other.sortable_term)
    }
}

impl Eq for RdfFusionEncodings {}

impl Hash for RdfFusionEncodings {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(Arc::as_ptr(&self.plain_term) as usize);
        state.write_usize(Arc::as_ptr(&self.typed_value) as usize);
        if let Some(object_id) = &self.object_id {
            state.write_usize(Arc::as_ptr(object_id) as usize);
        }
        state.write_usize(Arc::as_ptr(&self.sortable_term) as usize);
    }
}
