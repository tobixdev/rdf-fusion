use crate::scalar::sparql_op_impl::{ClosureSparqlOpImpl, ScalarSparqlOpImpl};
use crate::scalar::{ScalarSparqlOp, ScalarSparqlOpSignature, SparqlOpArity};
use datafusion::arrow::array::Array;
use datafusion::arrow::compute::is_not_null;
use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::typed_value::{
    TYPED_VALUE_ENCODING, TypedValueArray, TypedValueArrayElementBuilder,
    TypedValueEncoding,
};
use rdf_fusion_encoding::{EncodingArray, EncodingDatum, EncodingScalar, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_extensions::functions::FunctionName;
use rdf_fusion_model::DFResult;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct BoundSparqlOp;

impl Default for BoundSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl BoundSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Bound);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for BoundSparqlOp {
    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn signature(&self) -> ScalarSparqlOpSignature {
        ScalarSparqlOpSignature::default_with_arity(SparqlOpArity::Fixed(1))
    }

    fn plain_term_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<PlainTermEncoding>>> {
        Some(Box::new(ClosureSparqlOpImpl::new(
            TYPED_VALUE_ENCODING.data_type(),
            |args| impl_bound_plain_term(&args.args[0]),
        )))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<TypedValueEncoding>>> {
        Some(Box::new(ClosureSparqlOpImpl::new(
            TYPED_VALUE_ENCODING.data_type(),
            |args| impl_bound_typed_value(&args.args[0]),
        )))
    }

    fn object_id_encoding_op(
        &self,
        _object_id_encoding: &ObjectIdEncoding,
    ) -> Option<Box<dyn ScalarSparqlOpImpl<ObjectIdEncoding>>> {
        Some(Box::new(ClosureSparqlOpImpl::<ObjectIdEncoding>::new(
            TYPED_VALUE_ENCODING.data_type(),
            |args| impl_bound_object_id(&args.args[0]),
        )))
    }
}

fn impl_bound_plain_term(
    array: &EncodingDatum<PlainTermEncoding>,
) -> DFResult<ColumnarValue> {
    match array {
        EncodingDatum::Array(array) => impl_bound_array(array.array().as_ref())
            .map(|array| ColumnarValue::Array(array.into_array())),
        EncodingDatum::Scalar(scalar, _) => {
            let array = scalar.to_array(1)?.into_array();
            impl_bound_array(array.as_ref())?
                .try_as_scalar(0)
                .map(|scalar| ColumnarValue::Scalar(scalar.into_scalar_value()))
        }
    }
}

fn impl_bound_typed_value(
    array: &EncodingDatum<TypedValueEncoding>,
) -> DFResult<ColumnarValue> {
    match array {
        EncodingDatum::Array(array) => impl_bound_array(array.array().as_ref())
            .map(|array| ColumnarValue::Array(array.into_array())),
        EncodingDatum::Scalar(scalar, _) => {
            let array = scalar.to_array(1)?.into_array();
            impl_bound_array(array.as_ref())?
                .try_as_scalar(0)
                .map(|scalar| ColumnarValue::Scalar(scalar.into_scalar_value()))
        }
    }
}

fn impl_bound_object_id(
    array: &EncodingDatum<ObjectIdEncoding>,
) -> DFResult<ColumnarValue> {
    match array {
        EncodingDatum::Array(array) => impl_bound_array(array.array().as_ref())
            .map(|array| ColumnarValue::Array(array.into_array())),
        EncodingDatum::Scalar(scalar, _) => {
            let array = scalar.to_array(1)?.into_array();
            impl_bound_array(array.as_ref())?
                .try_as_scalar(0)
                .map(|scalar| ColumnarValue::Scalar(scalar.into_scalar_value()))
        }
    }
}

fn impl_bound_array(array: &dyn Array) -> DFResult<TypedValueArray> {
    let result = is_not_null(array)?;
    assert_eq!(
        result.null_count(),
        0,
        "is_not_null should never return null"
    );

    let mut builder = TypedValueArrayElementBuilder::default();
    for value in result.values() {
        builder.append_boolean(value.into())?;
    }

    Ok(builder.finish())
}
