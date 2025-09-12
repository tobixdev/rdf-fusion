use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{NullaryArgs, ScalarSparqlOp};
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rand::Rng;
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::typed_value::encoders::DefaultTypedValueEncoder;
use rdf_fusion_encoding::{EncodingArray, TermEncoder, TermEncoding};
use rdf_fusion_model::{Numeric, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct RandSparqlOp;

impl Default for RandSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl RandSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Rand);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for RandSparqlOp {
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
                let mut rng = rand::rng();
                let values = (0..number_rows).map(|_| {
                    let value = rng.random::<f64>();
                    Ok(TypedValueRef::NumericLiteral(Numeric::Double(value.into())))
                });
                let array = DefaultTypedValueEncoder::encode_terms(values)?;
                Ok(ColumnarValue::Array(array.into_array()))
            },
        ))
    }
}
