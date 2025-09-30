use crate::scalar::dispatch::{
    dispatch_n_ary_object_id, dispatch_n_ary_plain_term, dispatch_n_ary_typed_value,
};
use crate::scalar::sparql_op_impl::{
    ScalarSparqlOpImpl, create_object_id_sparql_op_impl,
    create_plain_term_sparql_op_impl, create_typed_value_sparql_op_impl,
};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::ThinError;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct CoalesceSparqlOp;

impl Default for CoalesceSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CoalesceSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Coalesce);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for CoalesceSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Variadic)
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(create_typed_value_sparql_op_impl(|args| {
            dispatch_n_ary_typed_value(
                args.args.as_slice(),
                args.number_rows,
                |args| args.first().copied().ok_or(ThinError::ExpectedError),
                |args| {
                    args.iter()
                        .find_map(|arg| arg.ok())
                        .ok_or(ThinError::ExpectedError)
                },
            )
        }))
    }

    fn plain_term_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<PlainTermEncoding>>> {
        Some(create_plain_term_sparql_op_impl(|args| {
            dispatch_n_ary_plain_term(
                args.args.as_slice(),
                args.number_rows,
                |args| args.first().copied().ok_or(ThinError::ExpectedError),
                |args| {
                    args.iter()
                        .find_map(|arg| arg.ok())
                        .ok_or(ThinError::ExpectedError)
                },
            )
        }))
    }

    fn object_id_encoding_op(
        &self,
        object_id_encoding: &ObjectIdEncoding,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<ObjectIdEncoding>>> {
        let object_id_encoding = object_id_encoding.clone();
        Some(create_object_id_sparql_op_impl(
            &object_id_encoding.clone(),
            move |args| {
                dispatch_n_ary_object_id(
                    &object_id_encoding,
                    args.args.as_slice(),
                    args.number_rows,
                    |args| args.first().cloned().ok_or(ThinError::ExpectedError),
                    |args| {
                        args.iter()
                            .find_map(|arg| (*arg).ok())
                            .ok_or(ThinError::ExpectedError)
                    },
                )
            },
        ))
    }
}
