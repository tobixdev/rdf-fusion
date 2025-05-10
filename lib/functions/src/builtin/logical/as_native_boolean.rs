use crate::builtin::factory::GraphFusionBuiltinFactory;
use crate::builtin::BuiltinName;
use crate::DFResult;
use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::internal_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use graphfusion_encoding::typed_value::{TypedValueEncoding, TypedValueEncodingField};
use graphfusion_encoding::{EncodingName, TermEncoding};
use graphfusion_model::Term;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
struct AsNativeBooleanFactory;

impl GraphFusionBuiltinFactory for AsNativeBooleanFactory {
    fn name(&self) -> BuiltinName {
        BuiltinName::NativeBooleanAsTerm
    }

    fn encoding(&self) -> Vec<EncodingName> {
        vec![EncodingName::TypedValue]
    }

    fn create_with_args(&self, _constant_args: HashMap<String, Term>) -> DFResult<ScalarUDF> {
        Ok(ScalarUDF::new_from_impl(AsNativeBoolean::new(self.name())))
    }
}

#[derive(Debug)]
pub struct AsNativeBoolean {
    name: String,
    signature: Signature,
}

impl AsNativeBoolean {
    pub fn new(name: BuiltinName) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::new(
                TypeSignature::Exact(vec![TypedValueEncoding::data_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for AsNativeBoolean {
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
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return internal_err!("Unexpected numer of arguments in enc_as_native_boolean.");
        }

        let input = args.args[0].to_array(args.number_rows)?;
        let terms = TypedValueEncoding::try_new_array(input)?;
        let parts = terms.parts_as_ref();

        if parts.booleans.len() + parts.null_count != args.number_rows {
            return internal_err!(
                "Unexpected all elements to either be a boolean or null. expected: {}, actual: {}",
                args.number_rows,
                parts.booleans.len() + parts.null_count
            );
        }

        let result = parts
            .array
            .type_ids()
            .iter()
            .enumerate()
            .map(|(idx, tid)| {
                Some(if *tid == TypedValueEncodingField::Boolean.type_id() {
                    parts.booleans.value(parts.array.value_offset(idx))
                } else {
                    false
                })
            })
            .collect::<BooleanArray>();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}
