use rdf_fusion_api::RdfFusionContextView;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::object_id::{
    ObjectIdArray, ObjectIdEncoding, ObjectIdMapping, ObjectIdScalar,
};
use rdf_fusion_encoding::plain_term::{
    PLAIN_TERM_ENCODING, PlainTermArray, PlainTermScalar,
};
use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
use rdf_fusion_encoding::typed_value::TYPED_VALUE_ENCODING;
use rdf_fusion_encoding::{QuadStorageEncoding, RdfFusionEncodings};
use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
use rdf_fusion_logical::RdfFusionLogicalPlanBuilderContext;
use rdf_fusion_model::TermRef;
use std::sync::Arc;

pub fn create_context() -> RdfFusionLogicalPlanBuilderContext {
    let object_id_encoding = ObjectIdEncoding::new(Arc::new(ObjectIdDummyMapping {}));
    let encodings = RdfFusionEncodings::new(
        PLAIN_TERM_ENCODING,
        TYPED_VALUE_ENCODING,
        Some(object_id_encoding.clone()),
        SORTABLE_TERM_ENCODING,
    );
    let rdf_fusion_context = RdfFusionContextView::new(
        Arc::new(DefaultRdfFusionFunctionRegistry::new(encodings.clone())),
        encodings,
        QuadStorageEncoding::ObjectId(object_id_encoding),
    );
    RdfFusionLogicalPlanBuilderContext::new(rdf_fusion_context)
}

#[derive(Debug, Default)]
struct ObjectIdDummyMapping {}

impl ObjectIdMapping for ObjectIdDummyMapping {
    fn try_get_object_id(&self, _id: TermRef<'_>) -> Option<u64> {
        unimplemented!()
    }

    fn encode(&self, _id: TermRef<'_>) -> u64 {
        unimplemented!()
    }

    fn decode_array(&self, _array: &ObjectIdArray) -> DFResult<PlainTermArray> {
        unimplemented!()
    }

    fn decode_scalar(&self, _scalar: &ObjectIdScalar) -> DFResult<PlainTermScalar> {
        unimplemented!()
    }
}
