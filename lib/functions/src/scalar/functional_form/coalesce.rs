use crate::scalar::dispatch::{
    dispatch_n_ary_object_id, dispatch_n_ary_plain_term, dispatch_n_ary_typed_value,
};
use crate::scalar::sparql_op_impl::{
    SparqlOpImpl, create_object_id_sparql_op_impl, create_plain_term_sparql_op_impl,
    create_typed_value_sparql_op_impl,
};
use crate::scalar::{NAryArgs, ScalarSparqlOp};
use datafusion::logical_expr::Volatility;
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_model::ThinError;

#[derive(Debug)]
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
    type Args<TEncoding: TermEncoding> = NAryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<TypedValueEncoding>>>> {
        Some(create_typed_value_sparql_op_impl(
            |NAryArgs(args, number_rows)| {
                dispatch_n_ary_typed_value(
                    &args,
                    number_rows,
                    |args| args.first().copied().ok_or(ThinError::ExpectedError),
                    |args| {
                        args.iter()
                            .find_map(|arg| arg.ok())
                            .ok_or(ThinError::ExpectedError)
                    },
                )
            },
        ))
    }

    fn plain_term_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<PlainTermEncoding>>>> {
        Some(create_plain_term_sparql_op_impl(
            |NAryArgs(args, number_rows)| {
                dispatch_n_ary_plain_term(
                    &args,
                    number_rows,
                    |args| args.first().copied().ok_or(ThinError::ExpectedError),
                    |args| {
                        args.iter()
                            .find_map(|arg| arg.ok())
                            .ok_or(ThinError::ExpectedError)
                    },
                )
            },
        ))
    }

    fn object_id_encoding_op(
        &self,
        object_id_encoding: &ObjectIdEncoding,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<ObjectIdEncoding>>>> {
        let object_id_encoding = object_id_encoding.clone();
        Some(create_object_id_sparql_op_impl(
            &object_id_encoding.clone(),
            move |NAryArgs(args, number_rows)| {
                dispatch_n_ary_object_id(
                    &object_id_encoding,
                    &args,
                    number_rows,
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
