use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{
    CompatibleStringArgs, LanguageStringRef, SimpleLiteralRef, StringLiteralRef,
    ThinError, TypedValueRef,
};

/// Implementation of the SPARQL `strbefore` function.
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct StrBeforeSparqlOp;

impl Default for StrBeforeSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrBeforeSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::StrBefore);

    /// Creates a new [StrBeforeSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for StrBeforeSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn details(&self) -> ScalarSparqlOpDetails {
        ScalarSparqlOpDetails::default_with_arity(SparqlOpArity::Fixed(2))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            dispatch_binary_typed_value(
                &args.args[0],
                &args.args[1],
                |lhs_value, rhs_value| {
                    let lhs_value = StringLiteralRef::try_from(lhs_value)?;
                    let rhs_value = StringLiteralRef::try_from(rhs_value)?;

                    let args = CompatibleStringArgs::try_from(lhs_value, rhs_value)?;

                    let value = if let Some(position) = args.lhs.find(args.rhs) {
                        &args.lhs[..position]
                    } else {
                        return Ok(TypedValueRef::SimpleLiteral(SimpleLiteralRef {
                            value: "",
                        }));
                    };

                    Ok(match args.language {
                        None => TypedValueRef::SimpleLiteral(SimpleLiteralRef { value }),
                        Some(language) => {
                            TypedValueRef::LanguageStringLiteral(LanguageStringRef {
                                value,
                                language,
                            })
                        }
                    })
                },
                |_, _| ThinError::expected(),
            )
        }))
    }
}
