use rdf_fusion_api::functions::BuiltinName;
use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::sparql_op_impl::{create_typed_value_sparql_op_impl, SparqlOpImpl};
use crate::scalar::{ScalarSparqlOp, UnaryArgs};
use rdf_fusion_api::functions::FunctionName;
use datafusion::logical_expr::Volatility;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_model::{Iri, NamedNode, ThinError, TypedValue, TypedValueRef};

#[derive(Debug)]
pub struct IriSparqlOp {
    base_iri: Option<Iri<String>>,
}

impl IriSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Iri);

    pub fn new(base_iri: Option<Iri<String>>) -> Self {
        Self { base_iri }
    }
}

impl ScalarSparqlOp for IriSparqlOp {
    type Args<TEncoding: TermEncoding> = UnaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<TypedValueEncoding>>>> {
        let base_iri = self.base_iri.clone();
        Some(create_typed_value_sparql_op_impl(move |UnaryArgs(arg)| {
            dispatch_unary_owned_typed_value(
                &arg,
                |value| match value {
                    TypedValueRef::NamedNode(named_node) => {
                        Ok(TypedValue::NamedNode(named_node.into_owned()))
                    }
                    TypedValueRef::SimpleLiteral(simple_literal) => {
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
