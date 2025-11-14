mod mem_quad_storage;
mod parquet;

use rdf_fusion_encoding::object_id::ObjectIdMapping;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
use rdf_fusion_encoding::typed_value::TYPED_VALUE_ENCODING;
use rdf_fusion_encoding::RdfFusionEncodings;
use rdf_fusion_extensions::functions::RdfFusionFunctionRegistryRef;
use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
use rdf_fusion_storage::memory::{MemObjectIdMapping, MemQuadStorage};
use std::sync::Arc;

fn create_storage() -> MemQuadStorage {
    let object_id_encoding = MemObjectIdMapping::new();
    MemQuadStorage::new(Arc::new(object_id_encoding), 10)
}

fn create_function_registry(
    object_id_mapping: Arc<dyn ObjectIdMapping>,
) -> RdfFusionFunctionRegistryRef {
    let encoding = RdfFusionEncodings::new(
        PLAIN_TERM_ENCODING,
        TYPED_VALUE_ENCODING,
        Some(object_id_mapping),
        SORTABLE_TERM_ENCODING,
    );
    Arc::new(DefaultRdfFusionFunctionRegistry::new(encoding))
}
