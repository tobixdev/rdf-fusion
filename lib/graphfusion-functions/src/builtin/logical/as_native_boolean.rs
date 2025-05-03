use crate::value_encoding::{RdfTermValueEncoding, RdfTermValueEncodingField};
use crate::{as_term_value_array, DFResult};
use datafusion::arrow::array::{as_boolean_array, Array, BooleanArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::internal_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct EncAsNativeBoolean {
    signature: Signature,
}

impl Default for EncAsNativeBoolean {
    fn default() -> Self {
        Self::new()
    }
}

impl EncAsNativeBoolean {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![RdfTermValueEncoding::datatype()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for EncAsNativeBoolean {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_as_native_boolean"
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
        let terms = as_term_value_array(&input)?;
        let boolean_array = as_boolean_array(terms.child(RdfTermValueEncodingField::Boolean.type_id()));
        let null_array = terms.child(RdfTermValueEncodingField::Null.type_id());

        if boolean_array.len() + null_array.len() != args.number_rows {
            return internal_err!(
                "Unexpected all elements to either be a boolean or null. expected: {}, actual: {}",
                args.number_rows,
                boolean_array.len() + null_array.len()
            );
        }

        let result = terms
            .type_ids()
            .iter()
            .enumerate()
            .map(|(idx, tid)| {
                Some(if *tid == RdfTermValueEncodingField::Boolean.type_id() {
                    boolean_array.value(terms.value_offset(idx))
                } else {
                    false
                })
            })
            .collect::<BooleanArray>();
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}
