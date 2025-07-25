use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::sortable_term::SortableTermEncoding;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use std::sync::Arc;

/// Holds a configuration instance for each RDF Fusion encoding.
///
/// This is an instance (as opposed to a type) as some encodings can be configured. At least
/// this is planned for the future. For each RDF Fusion instance, the encodings are fixed once it
/// is created.
#[derive(Debug, Clone)]
pub struct RdfFusionEncodingConfiguration {
    /// The [PlainTermEncoding] configuration.
    plain_term: Arc<PlainTermEncoding>,
    /// The [TypedValueEncoding] configuration.
    typed_value: Arc<TypedValueEncoding>,
    /// The [ObjectIdEncoding] configuration.
    object_id: Option<Arc<ObjectIdEncoding>>,
    /// The [SortableTermEncoding] configuration.
    sortable_term: Arc<SortableTermEncoding>,
}

impl RdfFusionEncodingConfiguration {
    /// Creates a new [RdfFusionEncodingConfiguration].
    pub fn new(
        plain_term: PlainTermEncoding,
        typed_value: TypedValueEncoding,
        object_id: Option<ObjectIdEncoding>,
        sortable_term: SortableTermEncoding,
    ) -> Self {
        Self {
            plain_term: Arc::new(plain_term),
            typed_value: Arc::new(typed_value),
            object_id: object_id.map(Arc::new),
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

    /// Provides a reference to the used [SortableTermEncoding].
    pub fn sortable_term(&self) -> &SortableTermEncoding {
        &self.sortable_term
    }
}
