use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{NullaryArgs, NullaryOrUnaryArgs, ScalarSparqlOp, UnaryArgs};
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::{EncodingArray, TermEncoding};
use rdf_fusion_encoding::typed_value::{TypedValueArrayBuilder, TypedValueEncoding};
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
    type Args<TEncoding: TermEncoding> = NullaryOrUnaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Volatile
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<TypedValueEncoding>>>> {
        Some(create_typed_value_sparql_op_impl(|args| match args {
            NullaryOrUnaryArgs::Nullary(NullaryArgs { number_rows }) => {
                let mut builder = TypedValueArrayBuilder::default();
                for _ in 0..number_rows {
                    builder.append_blank_node(BlankNode::default().as_ref())?;
                }
                Ok(ColumnarValue::Array(builder.finish().into_array()))
            }
            NullaryOrUnaryArgs::Unary(UnaryArgs(arg)) => dispatch_unary_typed_value(
                &arg,
                |value| match value {
                    TypedValueRef::SimpleLiteral(value) => {
                        let bnode = BlankNodeRef::new(value.value)?;
                        Ok(TypedValueRef::BlankNode(bnode))
                    }
                    _ => ThinError::expected(),
                },
                ThinError::expected,
            ),
        }))
    }
}
