use datafusion::arrow::array::{Array, as_boolean_array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use rdf_fusion_encoding::typed_value::{TYPED_VALUE_ENCODING, TypedValueArrayBuilder};
use rdf_fusion_encoding::{EncodingArray, TermEncoding};
use rdf_fusion_extensions::functions::BuiltinName;
use rdf_fusion_model::DFResult;
use std::any::Any;
use std::hash::{Hash, Hasher};

pub fn native_boolean_as_term() -> ScalarUDF {
    let udf_impl = NativeBooleanAsTerm::new();
    ScalarUDF::new_from_impl(udf_impl)
}

#[derive(Debug, Eq)]
struct NativeBooleanAsTerm {
    name: String,
    signature: Signature,
}

impl NativeBooleanAsTerm {
    pub fn new() -> Self {
        Self {
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

        Ok(ColumnarValue::Array(builder.finish().into_array()))
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
