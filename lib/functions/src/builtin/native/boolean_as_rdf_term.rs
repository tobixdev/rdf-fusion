use datafusion::arrow::array::{Array, as_boolean_array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_encoding::typed_value::{TYPED_VALUE_ENCODING, TypedValueArrayBuilder};
use std::any::Any;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

pub fn native_boolean_as_term() -> Arc<ScalarUDF> {
    let udf_impl = NativeBooleanAsTerm::new();
    Arc::new(ScalarUDF::new_from_impl(udf_impl))
}

#[derive(Debug)]
struct NativeBooleanAsTerm {
    name: String,
    signature: Signature,
}

impl NativeBooleanAsTerm {
    pub fn new() -> Self {
        Self {
            name: BuiltinName::NativeBooleanAsTypedValue.to_string(),
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
        Ok(TYPED_VALUE_ENCODING.data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("Unexpected number of arguments");
        }

        let arg = &args.args[0];
        if arg.data_type() != DataType::Boolean {
            return exec_err!("Unexpected argument type: {:?}", arg.data_type());
        }

        // Performance could be optimized here
        let arg = arg.to_array(args.number_rows)?;
        let arg = as_boolean_array(&arg);
        let mut builder = TypedValueArrayBuilder::default();
        for i in 0..args.number_rows {
            if arg.is_null(i) {
                builder.append_null()?;
            } else {
                builder.append_boolean(arg.value(i).into())?;
            }
        }

        Ok(ColumnarValue::Array(builder.finish()))
    }

    fn hash_value(&self) -> u64 {
        // Remove once https://github.com/apache/datafusion/pull/16977 is in release
        let hasher = &mut DefaultHasher::new();
        self.as_any().type_id().hash(hasher);
        hasher.finish()
    }
}
