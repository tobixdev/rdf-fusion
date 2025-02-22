use crate::encoded::dispatch::EncSimpleLiteral;
use crate::encoded::dispatch_binary::{dispatch_binary, EncScalarBinaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use std::any::Any;

#[derive(Debug)]
pub struct EncStrLang {
    signature: Signature,
}

impl EncStrLang {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type(), EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarBinaryUdf for EncStrLang {
    type ArgLhs<'lhs> = EncSimpleLiteral<'lhs>;
    type ArgRhs<'rhs> = EncSimpleLiteral<'rhs>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(
        collector: &mut Self::Collector,
        lhs: &Self::ArgLhs<'_>,
        rhs: &Self::ArgRhs<'_>,
    ) -> DFResult<()> {
        collector.append_string(lhs.0, Some(rhs.0))?;
        Ok(())
    }

    fn evaluate_error(collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}


impl ScalarUDFImpl for EncStrLang {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_strlang"
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
        dispatch_binary::<EncStrLang>(args, number_rows)
    }
}
