use crate::functions::RdfFusionFunctionRegistryRef;
use rdf_fusion_encoding::{QuadStorageEncoding, RdfFusionEncodings};

/// Represents a view of an RDF Fusion context.
///
/// This view can be passed into other parts of the engine that require information on the current
/// configuration of the engine but do not want to directly depend on the central context struct.
#[derive(Debug, Clone)]
pub struct RdfFusionContextView {
    /// Holds references to the registered built-in functions.
    functions: RdfFusionFunctionRegistryRef,
    /// Provides information on the encodings used in the engine.
    encodings: RdfFusionEncodings,
    /// The storage engine that backs this instance.
    ///
    /// This is important for deciding the output type of operator that match quad patterns.
    storage_encoding: QuadStorageEncoding,
}

impl RdfFusionContextView {
    /// Creates a new [RdfFusionContextView].
    pub fn new(
        functions: RdfFusionFunctionRegistryRef,
        encodings: RdfFusionEncodings,
        storage_encoding: QuadStorageEncoding,
    ) -> Self {
        Self {
            functions,
            encodings,
            storage_encoding,
        }
    }

    /// Provides a reference to the used [RdfFusionFunctionRegistry].
    pub fn functions(&self) -> &RdfFusionFunctionRegistryRef {
        &self.functions
    }

    /// Provides a reference to the used [RdfFusionEncodings].
    pub fn encodings(&self) -> &RdfFusionEncodings {
        &self.encodings
    }

    /// Provides a reference to the used [QuadStorageEncoding].
    pub fn storage_encoding(&self) -> &QuadStorageEncoding {
        &self.storage_encoding
    }
}
