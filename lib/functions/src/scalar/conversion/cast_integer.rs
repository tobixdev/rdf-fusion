use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::RdfFusionEncodings;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{Integer, Numeric, ThinError, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct CastIntegerSparqlOp;

impl Default for CastIntegerSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CastIntegerSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::CastInteger);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for CastIntegerSparqlOp {
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
                    &args.encoding,
                    &args.args[0],
                    |value| {
                        let converted = match value {
                            TypedValueRef::BooleanLiteral(v) => Integer::from(v),
                            TypedValueRef::SimpleLiteral(v) => v.value.parse()?,
                            TypedValueRef::NumericLiteral(numeric) => match numeric {
                                Numeric::Int(v) => Integer::from(v),
                                Numeric::Integer(v) => v,
                                Numeric::Float(v) => Integer::try_from(v)?,
                                Numeric::Double(v) => Integer::try_from(v)?,
                                Numeric::Decimal(v) => Integer::try_from(v)?,
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
