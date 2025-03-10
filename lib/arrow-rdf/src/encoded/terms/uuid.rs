use crate::encoded::EncTerm;
use crate::DFResult;
use datafusion::arrow::array::GenericStringArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::internal_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug)]
pub struct EncUuid {
    signature: Signature,
}

impl EncUuid {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Nullary, Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for EncUuid {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_uuid"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(EncTerm::term_type())
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        if args.len() != 0 {
            return internal_err!("Unexpected number of arguments");
        }

        let values =
            std::iter::repeat_with(|| format!("urn:uuid:{}", Uuid::new_v4())).take(number_rows);
        let array = GenericStringArray::<i32>::from_iter_values(values);
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}
