use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::typed_value::{TypedValueArrayBuilder, TypedValueEncoding};
use rdf_fusion_model::{BlankNode, BlankNodeRef, ThinError, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
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
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn details(&self) -> ScalarSparqlOpDetails {
        ScalarSparqlOpDetails {
            volatility: Volatility::Volatile,
            arity: SparqlOpArity::OneOf(vec![
                SparqlOpArity::Nullary,
                SparqlOpArity::Fixed(1),
            ]),
        }
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            match args.args.len() {
                0 => {
                    let mut builder = TypedValueArrayBuilder::default();
                    for _ in 0..args.number_rows {
                        builder.append_blank_node(BlankNode::default().as_ref())?;
                    }
                    Ok(ColumnarValue::Array(builder.finish().into_array()))
                }
                1 => dispatch_unary_typed_value(
                    &args.args[0],
                    |value| match value {
                        TypedValueRef::SimpleLiteral(value) => {
                            let bnode = BlankNodeRef::new(value.value)?;
                            Ok(TypedValueRef::BlankNode(bnode))
                        }
                        _ => ThinError::expected(),
                    },
                    ThinError::expected,
                ),
                _ => unreachable!("Invalid number of arguments"),
            }
        }))
    }
}
