use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::internal_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use rand::random;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct EncRand {
    signature: Signature,
}

impl EncRand {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Nullary, Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for EncRand {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_rand"
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

        let mut builder = EncRdfTermBuilder::new();
        for _ in 0..number_rows {
            builder.append_double(random::<f64>().into())?;
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()?)))
    }
}
