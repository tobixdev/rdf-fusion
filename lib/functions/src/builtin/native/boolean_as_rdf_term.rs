use datafusion::arrow::array::{Array, BooleanArray, as_boolean_array};
use datafusion::arrow::datatypes::DataType;
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

pub fn native_boolean_as_term(encoding: TypedValueEncodingRef) -> ScalarUDF {
    let udf_impl = NativeBooleanAsTerm::new(encoding);
    ScalarUDF::new_from_impl(udf_impl)
}

#[derive(Debug, Eq)]
struct NativeBooleanAsTerm {
    encoding: TypedValueEncodingRef,
    name: String,
    signature: Signature,
}

impl NativeBooleanAsTerm {
    pub fn new(encoding: TypedValueEncodingRef) -> Self {
        Self {
            encoding,
            name: BuiltinName::NativeBooleanAsTerm.to_string(),
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Boolean]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for NativeBooleanAsTerm {
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

        let arg = &args.args[0];
        if arg.data_type() != DataType::Boolean {
            return exec_err!("Unexpected argument type: {:?}", arg.data_type());
        }

        let arg = arg.to_array(args.number_rows)?;
        let bool_arg = as_boolean_array(&arg);

        let result = match bool_arg.nulls() {
            None => {
                let builder = TypedValueArrayBuilder::new_with_single_type(
                    Arc::clone(&self.encoding),
                    TypedValueEncodingField::Boolean.into(),
                    bool_arg.len(),
                )?;

                builder.with_booleans(Arc::clone(&arg)).finish()?
            }
            Some(nulls) => {
                let values = bool_arg.iter().filter(Option::is_some).collect::<Vec<_>>();
                let values = BooleanArray::from(values);

                let builder = TypedValueArrayBuilder::new_with_nullable_single_type(
                    Arc::clone(&self.encoding),
                    TypedValueEncodingField::Boolean.into(),
                    nulls,
                )?;

                builder.with_booleans(Arc::new(values)).finish()?
            }
        };

        Ok(ColumnarValue::Array(result.into_array_ref()))
    }
}

impl Hash for NativeBooleanAsTerm {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_any().type_id().hash(state);
    }
}

impl PartialEq for NativeBooleanAsTerm {
    fn eq(&self, other: &Self) -> bool {
        self.as_any().type_id() == other.as_any().type_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::ArrayRef;
    use datafusion::arrow::datatypes::{Field, FieldRef};
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
    use insta::assert_debug_snapshot;
    use rdf_fusion_encoding::typed_value::TypedValueEncoding;
    use std::sync::Arc;

    #[test]
    fn test_native_boolean_as_term_no_nulls() {
        let encoding = Arc::new(TypedValueEncoding::new());

        let values = Arc::new(BooleanArray::from(vec![false, true, false])) as ArrayRef;
        let result = test_native_boolean_as_term(&encoding, values)
            .to_array(3)
            .unwrap();

        let typed_value_array = encoding.try_new_array(result).unwrap();
        let parts = typed_value_array.parts_as_ref();

        assert_eq!(parts.array.len(), 3);
        assert_eq!(parts.null_count, 0);
        assert_eq!(parts.booleans.len(), 3);
        assert_debug_snapshot!(parts.booleans, @r"
        BooleanArray
        [
          false,
          true,
          false,
        ]
        ")
    }

    #[test]
    fn test_native_boolean_as_term_with_nulls() {
        let encoding = Arc::new(TypedValueEncoding::new());

        let values = Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(true),
            Some(false),
        ])) as ArrayRef;
        let result = test_native_boolean_as_term(&encoding, values)
            .to_array(4)
            .unwrap();

        let typed_value_array = encoding.try_new_array(result).unwrap();
        let parts = typed_value_array.parts_as_ref();

        assert_eq!(parts.array.len(), 4);
        assert_eq!(parts.null_count, 1);
        assert_eq!(parts.booleans.len(), 3);
        assert_debug_snapshot!(parts.booleans, @r"
        BooleanArray
        [
          true,
          true,
          false,
        ]
        ")
    }

    fn test_native_boolean_as_term(
        encoding: &TypedValueEncodingRef,
        input: ArrayRef,
    ) -> ColumnarValue {
        let udf = NativeBooleanAsTerm::new(Arc::clone(&encoding));
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
