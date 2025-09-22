use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{SimpleLiteral, ThinError, TypedValue, TypedValueRef};
use sha1::{Digest, Sha1};

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct Sha1SparqlOp;

impl Default for Sha1SparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha1SparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Sha1);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for Sha1SparqlOp {
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

                    let mut hasher = Sha1::new();
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
