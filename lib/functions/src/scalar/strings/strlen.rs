use crate::scalar::dispatch::dispatch_unary_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{Numeric, ThinError, TypedValueRef};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct StrLenSparqlOp;

impl Default for StrLenSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrLenSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::StrLen);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for StrLenSparqlOp {
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
                    let string = match value {
                        TypedValueRef::SimpleLiteral(value) => value.value,
                        TypedValueRef::LanguageStringLiteral(value) => value.value,
                        _ => return ThinError::expected(),
                    };
                    let value: i64 = string.chars().count().try_into()?;
                    Ok(TypedValueRef::NumericLiteral(Numeric::Integer(
                        value.into(),
                    )))
                },
                ThinError::expected,
            )
        }))
    }
}
