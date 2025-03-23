use crate::DFResult;
use datafusion::arrow::array::{as_boolean_array, Array, BooleanBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use std::ops::Not;
use std::sync::Arc;

#[derive(Debug)]
pub struct EncAnd {
    signature: Signature,
}

impl EncAnd {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Boolean, DataType::Boolean]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for EncAnd {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_and"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        let lhs = args[0].to_array(number_rows)?;
        let rhs = args[1].to_array(number_rows)?;

        let lhs = as_boolean_array(&lhs);
        let rhs = as_boolean_array(&rhs);

        let mut builder = BooleanBuilder::with_capacity(number_rows);
        for i in 0..number_rows {
            let lhs = lhs.is_null(i).not().then(|| lhs.value(i));
            let rhs = rhs.is_null(i).not().then(|| rhs.value(i));

            let result = match (lhs, rhs) {
                (Some(lhs), Some(rhs)) => Some(lhs && rhs),
                (Some(false), _) => Some(false),
                (_, Some(false)) => Some(false),
                _ => None,
            };
            builder.append_option(result)
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}
