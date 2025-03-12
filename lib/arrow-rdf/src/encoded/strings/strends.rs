use crate::datatypes::{CompatibleStringArgs, RdfStringLiteral};
use crate::encoded::dispatch_binary::{dispatch_binary, EncScalarBinaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

#[derive(Debug)]
pub struct EncStrEnds {
    signature: Signature,
}

impl EncStrEnds {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type(); 2]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarBinaryUdf for EncStrEnds {
    type ArgLhs<'lhs> = RdfStringLiteral<'lhs>;
    type ArgRhs<'lhs> = RdfStringLiteral<'lhs>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(
        collector: &mut Self::Collector,
        arg_lhs: &Self::ArgLhs<'_>,
        arg_rhs: &Self::ArgRhs<'_>,
    ) -> DFResult<()> {
        if !CompatibleStringArgs::try_from(arg_lhs, arg_rhs).is_ok() {
            collector.append_null()?;
            return Ok(());
        }

        let result = arg_lhs.0.ends_with(arg_rhs.0);
        collector.append_boolean(result)?;
        Ok(())
    }

    fn evaluate_error(collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncStrEnds {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_strends"
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
        dispatch_binary::<EncStrEnds>(args, number_rows)
    }
}
