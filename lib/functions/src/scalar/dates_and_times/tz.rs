use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{SimpleLiteral, ThinError, TypedValue, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct TzSparqlOp;

impl Default for TzSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl TzSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Tz);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for TzSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn details(&self) -> ScalarSparqlOpDetails {
        ScalarSparqlOpDetails::default_with_arity(SparqlOpArity::Fixed(1))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            dispatch_unary_owned_typed_value(
                &args.args[0],
                |value| {
                    let tz = match value {
                        TypedValueRef::DateTimeLiteral(v) => v
                            .timezone_offset()
                            .map(|offset| offset.to_string())
                            .unwrap_or_default(),
                        _ => return ThinError::expected(),
                    };
                    Ok(TypedValue::SimpleLiteral(SimpleLiteral { value: tz }))
                },
                ThinError::expected,
            )
        }))
    }
}
