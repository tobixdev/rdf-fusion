use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rand::Rng;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::typed_value::encoders::DefaultTypedValueEncoder;
use rdf_fusion_encoding::{EncodingArray, RdfFusionEncodings, TermEncoder};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
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
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature {
            volatility: Volatility::Volatile,
            arity: SparqlOpArity::Nullary,
        }
    }

    fn typed_value_encoding_op(
        &self,
        encodings: &RdfFusionEncodings,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(
            encodings.typed_value(),
            |args| {
                let mut rng = rand::rng();
                let values = (0..args.number_rows).map(|_| {
                    let value = rng.random::<f64>();
                    Ok(TypedValueRef::NumericLiteral(Numeric::Double(value.into())))
                });
                let array = args.encoding.default_encoder().encode_terms(values)?;
                Ok(ColumnarValue::Array(array.into_array_ref()))
            },
        ))
    }
}
