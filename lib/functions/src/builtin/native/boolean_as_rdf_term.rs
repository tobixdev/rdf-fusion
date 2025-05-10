use crate::builtin::factory::GraphFusionBuiltinFactory;
use crate::builtin::BuiltinName;
use crate::DFResult;
use datafusion::arrow::array::{as_boolean_array, Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use graphfusion_encoding::value_encoding::{TypedValueArrayBuilder, TypedValueEncoding};
use graphfusion_encoding::{EncodingName, TermEncoding};
use graphfusion_model::Term;
use std::any::Any;
use std::collections::HashMap;

#[derive(Debug)]
struct BooleanAsRdfTermFactory;

impl GraphFusionBuiltinFactory for BooleanAsRdfTermFactory {
    fn name(&self) -> BuiltinName {
        BuiltinName::NativeBooleanAsTerm
    }

    fn encoding(&self) -> Vec<EncodingName> {
        vec![EncodingName::TypedValue]
    }

    fn create_with_args(&self, _constant_args: HashMap<String, Term>) -> DFResult<ScalarUDF> {
        Ok(ScalarUDF::new_from_impl(BooleanAsRdfTerm::new(self.name())))
    }
}

#[derive(Debug)]
pub struct BooleanAsRdfTerm {
    name: String,
    signature: Signature,
}

impl BooleanAsRdfTerm {
    pub fn new(name: BuiltinName) -> Self {
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
