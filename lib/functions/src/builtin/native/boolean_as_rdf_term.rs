use crate::builtin::BuiltinName;
use crate::factory::GraphFusionUdfFactory;
use crate::{DFResult, FunctionName};
use datafusion::arrow::array::{as_boolean_array, Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use graphfusion_encoding::typed_value::{TypedValueArrayBuilder, TypedValueEncoding};
use graphfusion_encoding::{EncodingName, TermEncoding};
use graphfusion_model::Term;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct BooleanAsRdfTermTypedValueFactory;

impl GraphFusionUdfFactory for BooleanAsRdfTermTypedValueFactory {
    fn name(&self) -> FunctionName {
        FunctionName::Builtin(BuiltinName::NativeBooleanAsTerm)
    }

    fn encoding(&self) -> Vec<EncodingName> {
        vec![EncodingName::TypedValue]
    }

    fn create_with_args(&self, _constant_args: HashMap<String, Term>) -> DFResult<Arc<ScalarUDF>> {
        let udf = ScalarUDF::new_from_impl(BooleanAsRdfTerm::new(self.name()));
        Ok(Arc::new(udf))
    }
}

#[derive(Debug)]
pub struct BooleanAsRdfTerm {
    name: String,
    signature: Signature,
}

impl BooleanAsRdfTerm {
    pub fn new(name: FunctionName) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Boolean]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for BooleanAsRdfTerm {
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
        Ok(TypedValueEncoding::data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
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
}
