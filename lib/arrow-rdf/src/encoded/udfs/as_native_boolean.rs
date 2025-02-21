use crate::encoded::{EncTerm, EncTermField};
use crate::{as_enc_term_array, DFResult};
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::internal_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct EncAsNativeBoolean {
    signature: Signature,
}

impl EncAsNativeBoolean {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
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

    fn invoke_batch(&self, args: &[ColumnarValue], number_rows: usize) -> DFResult<ColumnarValue> {
        if args.len() != 1 {
            return internal_err!("Unexpected numer of arguments in enc_as_native_boolean.");
        }

        let input = args[0].to_array(number_rows)?;
        let terms = as_enc_term_array(&input)?;
        let boolean_array = terms.child(EncTermField::Boolean.type_id());

        if boolean_array.len() != number_rows {
            return internal_err!(
                "Unexpected number of boolean elements in enc_as_native_boolean. expected: {}, actual: {}", number_rows, boolean_array.len()
            );
        }

        if boolean_array.null_count() > 0 {
            return internal_err!("Unexpected nulls in enc_as_native_boolean.");
        }

        Ok(ColumnarValue::Array(Arc::clone(boolean_array)))
    }
}
