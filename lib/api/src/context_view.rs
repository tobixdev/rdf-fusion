use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_encoding::sortable_term::SortableTermEncoding;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use crate::functions::RdfFusionFunctionRegistryRef;


/// Holds a configuration instance for each RDF Fusion encoding.
///
/// This is an instance (as opposed to a type) as some encodings can be configured. At least
/// this is planned for the future. For each RDF Fusion instance, the encodings are fixed once it
/// is created.
struct RdfFusionEncodingConfiguration {
    /// The [PlainTermEncoding] configuration.
    plain_term: PlainTermEncoding,
    /// The [TypedValueEncoding] configuration.
    typed_value: TypedValueEncoding,
    /// The [ObjectIdEncoding] configuration.
    object_id_encoding: ObjectIdEncoding,
    /// The [SortableTermEncoding] configuration.
    sortable_encoding: SortableTermEncoding
}

/// Represents a view of an RDF Fusion context.
///
/// This view can be passed into other parts of the engine that require information on the current
/// configuration of the engine but do not want to directly depend on the central context struct.
pub struct RdfFusionContextView {
    /// Holds references to the registered built-in functions.
    functions: RdfFusionFunctionRegistryRef,
    /// Provides information on the encodings used in the engine.
    encodings: RdfFusionEncodingConfiguration,
    /// The storage engine that backs this instance.
    ///
    /// This is important for deciding the output type of operator that match quad patterns.
    storage_encoding: QuadStorageEncoding,
}