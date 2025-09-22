use crate::scalar::dispatch::dispatch_binary_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::{LiteralRef, ThinError, TypedValueRef};

/// TODO
#[derive(Debug, Hash, PartialEq, Eq)]
pub struct StrDtSparqlOp;

impl Default for StrDtSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrDtSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::StrDt);

    /// Creates a new [StrDtSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for StrDtSparqlOp {
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
                    if let (
                        TypedValueRef::SimpleLiteral(lhs_literal),
                        TypedValueRef::NamedNode(rhs_named_node),
                    ) = (lhs_value, rhs_value)
                    {
                        let plain_literal = LiteralRef::new_typed_literal(
                            lhs_literal.value,
                            rhs_named_node,
                        );
                        TypedValueRef::try_from(plain_literal)
                            .map_err(|_| ThinError::ExpectedError)
                    } else {
                        ThinError::expected()
                    }
                },
                |_, _| ThinError::expected(),
            )
        }))
    }
}
