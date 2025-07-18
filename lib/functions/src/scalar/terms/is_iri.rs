//! This is just an exemplary "optimized" version of a SPARQL operation.

use crate::builtin::BuiltinName;
use crate::scalar::sparql_op_impl::{create_typed_value_sparql_op_impl, SparqlOpImpl};
use crate::scalar::{ScalarSparqlOp, UnaryArgs};
use crate::FunctionName;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
    NullArray, StringArray, StructArray, UnionArray,
};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::{
    TypedValueArray, TypedValueArrayBuilder, TypedValueEncoding, TypedValueEncodingField,
};
use rdf_fusion_encoding::EncodingScalar;
use rdf_fusion_encoding::{EncodingDatum, TermEncoding};
use std::sync::Arc;

/// TODO
#[derive(Debug)]
pub struct IsIriSparqlOp;

impl Default for IsIriSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IsIriSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::IsIri);

    /// Creates a new [IsIriSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for IsIriSparqlOp {
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
        Some(create_typed_value_sparql_op_impl::<
            Self::Args<TypedValueEncoding>,
        >(|UnaryArgs(arg)| match arg {
            EncodingDatum::Array(array) => {
                let array = invoke_typed_value_array(array)?;
                Ok(ColumnarValue::Array(array))
            }
            EncodingDatum::Scalar(scalar, _) => {
                let array = scalar.to_array(1)?;
                let array_result = invoke_typed_value_array(array)?;
                let scalar_result = ScalarValue::try_from_array(&array_result, 0)?;
                Ok(ColumnarValue::Scalar(scalar_result))
            }
        }))
    }
}

fn invoke_typed_value_array(array: TypedValueArray) -> DFResult<ArrayRef> {
    let parts = array.parts_as_ref();

    // If we do not have nulls, we can simply scan and check the type ids.
    // TODO: Of course there should be some helper functions to make this more readable and
    //       share logic between IS_IRI, IS_LITERAL etc. :)
    if parts.null_count == 0 {
        let results = parts
            .array
            .type_ids()
            .iter()
            .map(|type_id| Some(*type_id == TypedValueEncodingField::NamedNode.type_id()))
            .collect::<BooleanArray>();

        let type_ids = (0..results.len() as i32)
            .map(|_| TypedValueEncodingField::Boolean.type_id())
            .collect();
        let offsets = (0..results.len() as i32)
            .into_iter()
            .collect::<ScalarBuffer<i32>>();

        return Ok(Arc::new(
            UnionArray::try_new(
                TypedValueEncoding::fields(),
                type_ids,
                Some(offsets),
                vec![
                    Arc::new(NullArray::new(0)),
                    Arc::new(StringArray::new_null(0)),
                    Arc::new(StringArray::new_null(0)),
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::string_fields(),
                        0,
                    )),
                    Arc::new(results),
                    Arc::new(Float32Array::new_null(0)),
                    Arc::new(Float64Array::new_null(0)),
                    Arc::new(Decimal128Array::new_null(0)),
                    Arc::new(Int32Array::new_null(0)),
                    Arc::new(Int64Array::new_null(0)),
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::timestamp_fields(),
                        0,
                    )),
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::timestamp_fields(),
                        0,
                    )),
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::timestamp_fields(),
                        0,
                    )),
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::duration_fields(),
                        0,
                    )),
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::typed_literal_fields(),
                        0,
                    )),
                ],
            )
            .expect("Fields and type match"),
        ));
    }

    // The regular path must distinguish between Null and Non-null values.
    let mut result = TypedValueArrayBuilder::default();
    for type_id in parts.array.type_ids() {
        if *type_id == TypedValueEncodingField::Null.type_id() {
            result.append_null()?;
        } else {
            let boolean = *type_id == TypedValueEncodingField::NamedNode.type_id();
            result.append_boolean(boolean.into())?;
        }
    }

    Ok(result.finish())
}
