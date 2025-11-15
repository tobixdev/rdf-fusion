use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{
    create_typed_value_sparql_op_impl, ScalarSparqlOpImpl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::RdfFusionEncodings;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{Double, Numeric, ThinError, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct CastDoubleSparqlOp;

impl Default for CastDoubleSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CastDoubleSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::CastDouble);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for CastDoubleSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Fixed(1))
    }

    fn typed_value_encoding_op(
        &self,
        encodings: &RdfFusionEncodings,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(
            encodings.typed_value(),
            |args| {
                dispatch_unary_typed_value(
                    &args.args[0],
                    |value| {
                        let converted = match value {
                            TypedValueRef::BooleanLiteral(v) => Double::from(v),
                            TypedValueRef::SimpleLiteral(v) => v.value.parse()?,
                            TypedValueRef::NumericLiteral(numeric) => match numeric {
                                Numeric::Int(v) => Double::from(v),
                                Numeric::Integer(v) => Double::from(v),
                                Numeric::Float(v) => Double::from(v),
                                Numeric::Double(v) => v,
                                Numeric::Decimal(v) => Double::from(v),
                            },
                            _ => return ThinError::expected(),
                        };
                        Ok(TypedValueRef::NumericLiteral(Numeric::from(converted)))
                    },
                    ThinError::expected,
                )
            },
        ))
    }
}
