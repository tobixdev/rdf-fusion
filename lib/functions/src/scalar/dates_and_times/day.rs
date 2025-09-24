use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{Numeric, ThinError, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct DaySparqlOp;

impl Default for DaySparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl DaySparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Day);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for DaySparqlOp {
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
            dispatch_unary_typed_value(
                &args.args[0],
                |value| {
                    if let TypedValueRef::DateTimeLiteral(dt) = value {
                        Ok(TypedValueRef::NumericLiteral(Numeric::Integer(
                            dt.day().into(),
                        )))
                    } else {
                        ThinError::expected()
                    }
                },
                ThinError::expected,
            )
        }))
    }
}
