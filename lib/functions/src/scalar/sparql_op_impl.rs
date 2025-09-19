use crate::scalar::ScalarSparqlOpArgs;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_model::DFResult;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::typed_value::{TYPED_VALUE_ENCODING, TypedValueEncoding};

pub trait SparqlOpImpl<TEncoding: TermEncoding> {
    /// TODO
    fn return_type(&self) -> DataType;

    /// TODO
    fn invoke(&self, args: ScalarSparqlOpArgs<TEncoding>) -> DFResult<ColumnarValue>;
}

pub struct ClosureSparqlOpImpl<TEncoding: TermEncoding> {
    return_type: DataType,
    closure: Box<dyn Fn(ScalarSparqlOpArgs<TEncoding>) -> DFResult<ColumnarValue>>,
}

impl<TEncoding: TermEncoding> ClosureSparqlOpImpl<TEncoding> {
    /// Create a new `ClosureSparqlOpImpl`.
    pub fn new(
        return_type: DataType,
        closure: impl Fn(ScalarSparqlOpArgs<TEncoding>) -> DFResult<ColumnarValue> + 'static,
    ) -> Self {
        Self {
            return_type,
            closure: Box::new(closure),
        }
    }
}

impl<TEncoding: TermEncoding> SparqlOpImpl<TEncoding> for ClosureSparqlOpImpl<TEncoding> {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn invoke(&self, args: ScalarSparqlOpArgs<TEncoding>) -> DFResult<ColumnarValue> {
        (self.closure)(args)
    }
}

pub fn create_plain_term_sparql_op_impl(
    closure: impl Fn(ScalarSparqlOpArgs<PlainTermEncoding>) -> DFResult<ColumnarValue>
    + 'static,
) -> Box<dyn SparqlOpImpl<PlainTermEncoding>> {
    Box::new(ClosureSparqlOpImpl {
        return_type: PlainTermEncoding::data_type(),
        closure: Box::new(closure),
    })
}

pub fn create_typed_value_sparql_op_impl(
    closure: impl Fn(ScalarSparqlOpArgs<TypedValueEncoding>) -> DFResult<ColumnarValue>
    + 'static,
) -> Box<dyn SparqlOpImpl<TypedValueEncoding>> {
    Box::new(ClosureSparqlOpImpl {
        return_type: TYPED_VALUE_ENCODING.data_type(),
        closure: Box::new(closure),
    })
}

pub fn create_object_id_sparql_op_impl(
    object_id_encoding: &ObjectIdEncoding,
    closure: impl Fn(ScalarSparqlOpArgs<ObjectIdEncoding>) -> DFResult<ColumnarValue>
    + 'static,
) -> Box<dyn SparqlOpImpl<ObjectIdEncoding>> {
    Box::new(ClosureSparqlOpImpl {
        return_type: object_id_encoding.data_type(),
        closure: Box::new(closure),
    })
}
