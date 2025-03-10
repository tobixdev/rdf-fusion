use crate::datatypes::{RdfStringLiteral, XsdInteger};
use crate::encoded::dispatch_binary::{dispatch_binary, EncScalarBinaryUdf};
use crate::encoded::dispatch_unary::EncScalarUnaryUdf;
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

#[derive(Debug)]
pub struct EncSubStr {
    signature: Signature,
}

impl EncSubStr {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::OneOf(vec![
                    TypeSignature::Exact(vec![EncTerm::term_type(); 2]),
                    TypeSignature::Exact(vec![EncTerm::term_type(); 3]),
                ]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarBinaryUdf for EncSubStr {
    type ArgLhs<'lhs> = RdfStringLiteral<'lhs>;
    type ArgRhs<'lhs> = XsdInteger;
    type Collector = EncRdfTermBuilder;

    fn evaluate(
        collector: &mut Self::Collector,
        arg_lhs: &Self::ArgLhs<'_>,
        arg_rhs: &Self::ArgRhs<'_>,
    ) -> DFResult<()> {
        match usize::try_from(arg_rhs.as_i64()) {
            Ok(index) => collector.append_string(&arg_lhs.0[index..], arg_lhs.1)?,
            Err(_) => exec_err!("Invalid index argument")?,
        };
        Ok(())
    }

    fn evaluate_error(collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncSubStr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_strlen"
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
        match args.len() {
            2 => dispatch_binary::<EncSubStr>(args, number_rows),
            3 => todo!(),
            _ => exec_err!("Unexpected number of arguments"),
        }
    }
}
