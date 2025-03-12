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
pub struct EncStrBefore {
    signature: Signature,
}

impl EncStrBefore {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::OneOf(vec![TypeSignature::Exact(vec![EncTerm::term_type(); 2])]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarBinaryUdf for EncStrBefore {
    type ArgLhs<'lhs> = RdfStringLiteral<'lhs>;
    type ArgRhs<'lhs> = RdfStringLiteral<'lhs>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(
        collector: &mut Self::Collector,
        arg_lhs: &Self::ArgLhs<'_>,
        arg_rhs: &Self::ArgRhs<'_>,
    ) -> DFResult<()> {
        let Ok(args) = CompatibleStringArgs::try_from(arg_lhs, arg_rhs) else {
            collector.append_null()?;
            return Ok(());
        };

        if let Some(position) = arg_lhs.0.find(arg_rhs.0) {
            collector.append_string(&args.lhs[..position], args.language)?;
        } else {
            collector.append_string("", args.language)?;
        }

        Ok(())
    }

    fn evaluate_error(collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncStrBefore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_strbefore"
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
        dispatch_binary::<EncStrBefore>(args, number_rows)
    }
}
