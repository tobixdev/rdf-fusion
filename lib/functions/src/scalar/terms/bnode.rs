use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::{NullaryArgs, NullaryOrUnaryArgs, ScalarSparqlOp, UnaryArgs};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::{TypedValueArrayBuilder, TypedValueEncoding};
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::{BlankNode, BlankNodeRef, ThinError, TypedValueRef};

#[derive(Debug)]
pub struct BNodeSparqlOp;

impl Default for BNodeSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl BNodeSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::BNode);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for BNodeSparqlOp {
    type Args<TEncoding: TermEncoding> = NullaryOrUnaryArgs<TypedValueEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn supported_encodings(&self) -> &[EncodingName] {
        &[EncodingName::TypedValue]
    }

    fn volatility(&self) -> Volatility {
        Volatility::Volatile
    }

    fn return_type(&self, _input_encoding: Option<EncodingName>) -> DFResult<DataType> {
        Ok(TypedValueEncoding::data_type())
    }

    fn invoke_typed_value_encoding(
        &self,
        args: Self::Args<TypedValueEncoding>,
    ) -> DFResult<ColumnarValue> {
        match args {
            NullaryOrUnaryArgs::Nullary(NullaryArgs { number_rows }) => {
                let mut builder = TypedValueArrayBuilder::default();
                for _ in 0..number_rows {
                    builder.append_blank_node(BlankNode::default().as_ref())?;
                }
                Ok(ColumnarValue::Array(builder.finish()))
            }
            NullaryOrUnaryArgs::Unary(UnaryArgs(arg)) => dispatch_unary_typed_value(
                &arg,
                |value| match value {
                    TypedValueRef::SimpleLiteral(value) => {
                        let bnode = BlankNodeRef::new(&value.value)?;
                        Ok(TypedValueRef::BlankNode(bnode))
                    }
                    _ => ThinError::expected(),
                },
                || ThinError::expected(),
            ),
        }
    }
}
