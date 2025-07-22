use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::typed_value::TYPED_VALUE_ENCODING;
use rdf_fusion_encoding::TermEncoding;

pub trait SparqlOpImpl<Args> {
    /// TODO
    fn return_type(&self) -> DataType;

    /// TODO
    fn invoke(&self, args: Args) -> DFResult<ColumnarValue>;
}

pub struct ClosureSparqlOpImpl<Args> {
    return_type: DataType,
    closure: Box<dyn Fn(Args) -> DFResult<ColumnarValue>>,
}

impl<Args> SparqlOpImpl<Args> for ClosureSparqlOpImpl<Args> {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }

    fn invoke(&self, args: Args) -> DFResult<ColumnarValue> {
        (self.closure)(args)
    }
}

pub fn create_plain_term_sparql_op_impl<Args: 'static>(
    closure: impl Fn(Args) -> DFResult<ColumnarValue> + 'static,
) -> Box<dyn SparqlOpImpl<Args>> {
    Box::new(ClosureSparqlOpImpl {
        return_type: PLAIN_TERM_ENCODING.data_type(),
        closure: Box::new(closure),
    })
}

pub fn create_typed_value_sparql_op_impl<Args: 'static>(
    closure: impl Fn(Args) -> DFResult<ColumnarValue> + 'static,
) -> Box<dyn SparqlOpImpl<Args>> {
    Box::new(ClosureSparqlOpImpl {
        return_type: TYPED_VALUE_ENCODING.data_type(),
        closure: Box::new(closure),
    })
}
