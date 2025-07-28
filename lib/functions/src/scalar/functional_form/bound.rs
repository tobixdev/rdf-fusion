use crate::scalar::sparql_op_impl::{ClosureSparqlOpImpl, SparqlOpImpl};
use crate::scalar::{ScalarSparqlOp, UnaryArgs};
use datafusion::arrow::array::Array;
use datafusion::arrow::compute::is_not_null;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::typed_value::{
    TYPED_VALUE_ENCODING, TypedValueArray, TypedValueArrayBuilder, TypedValueEncoding,
};
use rdf_fusion_encoding::{EncodingArray, EncodingDatum, EncodingScalar, TermEncoding};

#[derive(Debug)]
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
    type Args<TEncoding: TermEncoding> = UnaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn plain_term_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<PlainTermEncoding>>>> {
        Some(Box::new(
            ClosureSparqlOpImpl::<UnaryArgs<PlainTermEncoding>>::new(
                TYPED_VALUE_ENCODING.data_type(),
                Box::new(|UnaryArgs(args)| impl_bound_plain_term(&args)),
            ),
        ))
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<TypedValueEncoding>>>> {
        Some(Box::new(
            ClosureSparqlOpImpl::<UnaryArgs<TypedValueEncoding>>::new(
                TYPED_VALUE_ENCODING.data_type(),
                Box::new(|UnaryArgs(args)| impl_bound_typed_value(&args)),
            ),
        ))
    }

    fn object_id_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<ObjectIdEncoding>>>> {
        Some(Box::new(
            ClosureSparqlOpImpl::<UnaryArgs<ObjectIdEncoding>>::new(
                TYPED_VALUE_ENCODING.data_type(),
                Box::new(|UnaryArgs(args)| impl_bound_object_id(&args)),
            ),
        ))
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

    let mut builder = TypedValueArrayBuilder::default();
    for value in result.values() {
        builder.append_boolean(value.into())?;
    }

    TYPED_VALUE_ENCODING.try_new_array(builder.finish())
}
