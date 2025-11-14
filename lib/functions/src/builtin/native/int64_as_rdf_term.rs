use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::as_int64_array;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use rdf_fusion_encoding::typed_value::{
    TypedValueArrayBuilder, TypedValueEncodingField, TypedValueEncodingRef,
};
use rdf_fusion_encoding::{EncodingArray, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub fn native_int64_as_term() -> ScalarUDF {
    let udf_impl = NativeInt64AsTerm::new();
    ScalarUDF::new_from_impl(udf_impl)
}

#[derive(Debug, Eq)]
pub struct NativeInt64AsTerm {
    encoding: TypedValueEncodingRef,
    name: String,
    signature: Signature,
}

impl NativeInt64AsTerm {
    pub fn new(encoding: TypedValueEncodingRef) -> Self {
        Self {
            encoding,
            name: BuiltinName::NativeInt64AsTerm.to_string(),
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Int64]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for NativeInt64AsTerm {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for NativeInt64AsTerm {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(self.encoding.data_type().clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("Unexpected number of arguments");
        }

        let arg = args.args[0].to_array(args.number_rows)?;
        let int_arg = as_int64_array(&arg)?;

        let result = match int_arg.nulls() {
            None => {
                let builder = TypedValueArrayBuilder::new_with_single_type(
                    Arc::clone(&self.encoding),
                    TypedValueEncodingField::Integer.into(),
                    int_arg.len(),
                )?;

                builder.with_integers(Arc::clone(&arg)).finish()?
            }
            Some(nulls) => {
                let values = int_arg
                    .iter()
                    .filter(Option::is_some)
                    .collect::<Int64Array>();

                let builder = TypedValueArrayBuilder::new_with_nullable_single_type(
                    Arc::clone(&self.encoding),
                    TypedValueEncodingField::Integer.into(),
                    nulls,
                )?;

                builder.with_integers(Arc::new(values)).finish()?
            }
        };

        Ok(ColumnarValue::Array(result.into_array_ref()))
    }
}

impl Hash for NativeInt64AsTerm {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_any().type_id().hash(state);
    }
}

impl PartialEq for NativeInt64AsTerm {
    fn eq(&self, other: &Self) -> bool {
        self.as_any().type_id() == other.as_any().type_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{ArrayRef, Int64Array};
    use datafusion::arrow::datatypes::{Field, FieldRef};
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
    use insta::assert_debug_snapshot;
    use rdf_fusion_encoding::typed_value::TypedValueEncoding;
    use std::sync::Arc;

    #[test]
    fn test_native_int64_as_term_no_nulls() {
        let encoding = Arc::new(TypedValueEncoding::new());

        let values = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let result = invoke_native_int64_as_term(&encoding, values)
            .to_array(3)
            .unwrap();

        let typed_value_array = encoding.try_new_array(result).unwrap();
        let parts = typed_value_array.parts_as_ref();

        assert_eq!(parts.array.len(), 3);
        assert_eq!(parts.null_count, 0);
        assert_eq!(parts.integers.len(), 3);
        assert_debug_snapshot!(parts.integers, @r"
        PrimitiveArray<Int64>
        [
          1,
          2,
          3,
        ]
        ")
    }

    #[test]
    fn test_native_int64_as_term_with_nulls() {
        let encoding = Arc::new(TypedValueEncoding::new());

        let values = Arc::new(Int64Array::from(vec![Some(10), None, Some(-5), Some(0)]))
            as ArrayRef;
        let result = invoke_native_int64_as_term(&encoding, values)
            .to_array(4)
            .unwrap();

        let typed_value_array = encoding.try_new_array(result).unwrap();
        let parts = typed_value_array.parts_as_ref();

        assert_eq!(parts.array.len(), 4);
        assert_eq!(parts.null_count, 1);
        assert_eq!(parts.integers.len(), 3);
        assert_debug_snapshot!(parts.integers, @r"
        PrimitiveArray<Int64>
        [
          10,
          -5,
          0,
        ]
        ")
    }

    fn invoke_native_int64_as_term(
        encoding: &TypedValueEncodingRef,
        input: ArrayRef,
    ) -> ColumnarValue {
        let udf = NativeInt64AsTerm::new(Arc::clone(encoding));
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::clone(&input))],
            arg_fields: vec![FieldRef::new(Field::new(
                "input",
                input.data_type().clone(),
                true,
            ))],
            number_rows: input.len(),
            return_field: FieldRef::new(Field::new(
                "output",
                encoding.data_type().clone(),
                true,
            )),
            config_options: Arc::new(Default::default()),
        };
        udf.invoke_with_args(args).unwrap()
    }
}
