use crate::builtin::BuiltinName;
use crate::scalar::sparql_op_impl::{create_typed_value_sparql_op_impl, SparqlOpImpl};
use crate::scalar::{NullaryArgs, ScalarSparqlOp};
use crate::FunctionName;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_encoding::typed_value::encoders::DefaultTypedValueEncoder;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingArray, TermEncoder, TermEncoding};
use rdf_fusion_model::{SimpleLiteral, TypedValue};
use uuid::Uuid;

#[derive(Debug)]
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
    type Args<TEncoding: TermEncoding> = NullaryArgs;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Volatile
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<TypedValueEncoding>>>> {
        Some(create_typed_value_sparql_op_impl(
            |NullaryArgs { number_rows }| {
                let values = (0..number_rows)
                    .map(|_| {
                        let result = Uuid::new_v4().to_string();
                        TypedValue::SimpleLiteral(SimpleLiteral { value: result })
                    })
                    .collect::<Vec<_>>();
                let array = DefaultTypedValueEncoder::encode_terms(
                    values.iter().map(|result| Ok(result.as_ref())),
                )?;
                Ok(ColumnarValue::Array(array.into_array()))
            },
        ))
    }
}
