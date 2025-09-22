use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{ThinError, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct CastDateTimeSparqlOp;

impl Default for CastDateTimeSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CastDateTimeSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::CastDateTime);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for CastDateTimeSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn details(&self) -> ScalarSparqlOpDetails {
        ScalarSparqlOpDetails::default_with_arity(SparqlOpArity::Fixed(1))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|arg| {
            dispatch_unary_typed_value(
                &arg.args[0],
                |value| {
                    let converted = match value {
                        TypedValueRef::SimpleLiteral(v) => v.value.parse()?,
                        TypedValueRef::DateTimeLiteral(v) => v,
                        _ => return ThinError::expected(),
                    };
                    Ok(TypedValueRef::DateTimeLiteral(converted))
                },
                ThinError::expected,
            )
        }))
    }
}
