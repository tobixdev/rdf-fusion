use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::typed_value::encoders::DefaultTypedValueEncoder;
use rdf_fusion_encoding::{EncodingArray, TermEncoder};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{SimpleLiteral, TypedValue};
use uuid::Uuid;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct StrUuidSparqlOp;

impl Default for StrUuidSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrUuidSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::StrUuid);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for StrUuidSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn details(&self) -> ScalarSparqlOpDetails {
        ScalarSparqlOpDetails::default_with_arity(SparqlOpArity::Nullary)
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            let values = (0..args.number_rows)
                .map(|_| {
                    let result = Uuid::new_v4().to_string();
                    TypedValue::SimpleLiteral(SimpleLiteral { value: result })
                })
                .collect::<Vec<_>>();
            let array = DefaultTypedValueEncoder::encode_terms(
                values.iter().map(|result| Ok(result.as_ref())),
            )?;
            Ok(ColumnarValue::Array(array.into_array()))
        }))
    }
}
