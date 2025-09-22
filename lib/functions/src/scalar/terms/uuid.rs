use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::typed_value::encoders::DefaultTypedValueEncoder;
use rdf_fusion_encoding::{EncodingArray, TermEncoder};
use rdf_fusion_model::{NamedNode, TypedValue};
use uuid::Uuid;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct UuidSparqlOp;

impl Default for UuidSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl UuidSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Uuid);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for UuidSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn details(&self) -> ScalarSparqlOpDetails {
        ScalarSparqlOpDetails {
            volatility: Volatility::Volatile,
            arity: SparqlOpArity::Nullary,
        }
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            let values = (0..args.number_rows)
                .map(|_| {
                    let formatted = format!("urn:uuid:{}", Uuid::new_v4());
                    TypedValue::NamedNode(NamedNode::new_unchecked(formatted))
                })
                .collect::<Vec<_>>();
            let array = DefaultTypedValueEncoder::encode_terms(
                values.iter().map(|result| Ok(result.as_ref())),
            )?;
            Ok(ColumnarValue::Array(array.into_array()))
        }))
    }
}
