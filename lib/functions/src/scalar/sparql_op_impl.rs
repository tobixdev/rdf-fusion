use crate::scalar::ScalarSparqlOpArgs;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::typed_value::{TypedValueEncoding, TypedValueEncodingRef};
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_model::DFResult;

pub trait ScalarSparqlOpImpl<TEncoding: TermEncoding> {
    /// Returns the return type of this operation.
    fn return_type(&self) -> DataType;

    /// Invokes the operation on the given `args`.
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

impl<TEncoding: TermEncoding> ScalarSparqlOpImpl<TEncoding>
    for ClosureSparqlOpImpl<TEncoding>
{
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
) -> Box<dyn ScalarSparqlOpImpl<PlainTermEncoding>> {
    Box::new(ClosureSparqlOpImpl {
        return_type: PlainTermEncoding::data_type(),
        closure: Box::new(closure),
    })
}

pub fn create_typed_value_sparql_op_impl(
    encoding: &TypedValueEncodingRef,
    closure: impl Fn(ScalarSparqlOpArgs<TypedValueEncoding>) -> DFResult<ColumnarValue>
    + 'static,
) -> Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>> {
    Box::new(ClosureSparqlOpImpl {
        return_type: encoding.data_type().clone(),
        closure: Box::new(closure),
    })
}

pub fn create_object_id_sparql_op_impl(
    object_id_encoding: &ObjectIdEncoding,
    closure: impl Fn(ScalarSparqlOpArgs<ObjectIdEncoding>) -> DFResult<ColumnarValue>
    + 'static,
) -> Box<dyn ScalarSparqlOpImpl<ObjectIdEncoding>> {
    Box::new(ClosureSparqlOpImpl {
        return_type: object_id_encoding.data_type().clone(),
        closure: Box::new(closure),
    })
}
