use crate::builtin::native::ebv;
use crate::scalar::sparql_op_impl::{SparqlOpImpl, create_typed_value_sparql_op_impl};
use crate::scalar::{ScalarSparqlOp, UnaryArgs};
use datafusion::arrow::array::Array;
use datafusion::arrow::compute::not;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_api::functions::FunctionName;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::{
    TYPED_VALUE_ENCODING, TypedValueArray, TypedValueArrayBuilder, TypedValueEncoding,
};
use rdf_fusion_encoding::{EncodingArray, EncodingDatum, EncodingScalar, TermEncoding};

#[derive(Debug)]
pub struct NotSparqlOp;

impl Default for NotSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl NotSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Not);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for NotSparqlOp {
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
        Some(create_typed_value_sparql_op_impl(|UnaryArgs(arg)| {
            not_typed_value(arg)
        }))
    }
}

fn not_typed_value(arg: EncodingDatum<TypedValueEncoding>) -> DFResult<ColumnarValue> {
    match arg {
        EncodingDatum::Array(array) => not_typed_value_array(&array)
            .map(|res| ColumnarValue::Array(res.into_array())),
        EncodingDatum::Scalar(scalar, _) => {
            let array = scalar.to_array(1)?;
            not_typed_value_array(&array)?
                .try_as_scalar(0)
                .map(|res| ColumnarValue::Scalar(res.into_scalar_value()))
        }
    }
}

fn not_typed_value_array(array: &TypedValueArray) -> DFResult<TypedValueArray> {
    let booleans = ebv(array);
    let result = not(&booleans)?;

    if result.null_count() == 0 {
        let mut builder = TypedValueArrayBuilder::default();
        for value in result.values() {
            builder.append_boolean(value.into())?;
        }
        return TYPED_VALUE_ENCODING.try_new_array(builder.finish());
    }

    let mut builder = TypedValueArrayBuilder::default();
    for value in result.iter() {
        match value {
            None => {
                builder.append_null()?;
            }
            Some(value) => {
                builder.append_boolean(value.into())?;
            }
        }
    }

    TYPED_VALUE_ENCODING.try_new_array(builder.finish())
}
