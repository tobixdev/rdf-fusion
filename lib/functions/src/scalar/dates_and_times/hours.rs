use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::RdfFusionEncodings;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{Numeric, ThinError, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct HoursSparqlOp;

impl Default for HoursSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl HoursSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Hours);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for HoursSparqlOp {
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
                        if let TypedValueRef::DateTimeLiteral(dt) = value {
                            Ok(TypedValueRef::NumericLiteral(Numeric::Integer(
                                dt.hour().into(),
                            )))
                        } else {
                            ThinError::expected()
                        }
                    },
                    ThinError::expected,
                )
            },
        ))
    }
}
