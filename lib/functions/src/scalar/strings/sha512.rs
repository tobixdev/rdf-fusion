use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{SimpleLiteral, ThinError, TypedValue, TypedValueRef};
use sha2::{Digest, Sha512};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Sha512SparqlOp;

impl Default for Sha512SparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha512SparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Sha512);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for Sha512SparqlOp {
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
                    let string = match value {
                        TypedValueRef::SimpleLiteral(value) => value.value,
                        TypedValueRef::LanguageStringLiteral(value) => value.value,
                        _ => return ThinError::expected(),
                    };

                    let mut hasher = Sha512::new();
                    hasher.update(string);
                    let result = hasher.finalize();
                    let value = format!("{result:x}");

                    Ok(TypedValue::SimpleLiteral(SimpleLiteral { value }))
                },
                ThinError::expected,
            )
        }))
    }
}
