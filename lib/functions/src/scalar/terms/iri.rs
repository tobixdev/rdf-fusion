use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpDetails, SparqlOpArity};
use datafusion::logical_expr::Volatility;
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::{Iri, NamedNode, ThinError, TypedValue, TypedValueRef};

#[derive(Debug, Default, Hash, PartialEq, Eq)]
pub struct IriSparqlOp;

impl IriSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Iri);
}

impl ScalarSparqlOp for IriSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn details(&self) -> ScalarSparqlOpDetails {
        ScalarSparqlOpDetails {
            volatility: Volatility::Immutable,
            num_constant_args: 1,
            arity: SparqlOpArity::Fixed(1),
        }
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(move |args| {
            dispatch_unary_owned_typed_value(
                &args.args[0],
                |value| match value {
                    TypedValueRef::NamedNode(named_node) => {
                        Ok(TypedValue::NamedNode(named_node.into_owned()))
                    }
                    TypedValueRef::SimpleLiteral(simple_literal) => {
                        let base_iri: Option<Iri<String>> = todo!("base_iri");
                        let resolving_result = if let Some(base_iri) = &base_iri {
                            base_iri.resolve(simple_literal.value)?
                        } else {
                            Iri::parse(simple_literal.value.to_owned())?
                        };
                        Ok(TypedValue::NamedNode(NamedNode::from(resolving_result)))
                    }
                    _ => ThinError::expected(),
                },
                ThinError::expected,
            )
        }))
    }
}
