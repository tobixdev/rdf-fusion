use crate::datatypes::RdfStringLiteral;
use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_datafusion_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

#[derive(Debug)]
pub struct EncEncodeForUri {
    signature: Signature,
}

impl EncEncodeForUri {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarUnaryUdf for EncEncodeForUri {
    type Arg<'data> = RdfStringLiteral<'data>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        // Based on oxigraph/lib/spareval/src/eval.rs
        // MAybe we can use a library in the future?
        let mut result = Vec::with_capacity(value.0.len());
        for c in value.0.bytes() {
            match c {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    result.push(c)
                }
                _ => {
                    result.push(b'%');
                    let high = c / 16;
                    let low = c % 16;
                    result.push(if high < 10 {
                        b'0' + high
                    } else {
                        b'A' + (high - 10)
                    });
                    result.push(if low < 10 {
                        b'0' + low
                    } else {
                        b'A' + (low - 10)
                    });
                }
            }
        }

        let result = std::str::from_utf8(result.as_slice())
            .map_err(|_| exec_datafusion_err!("Result is not valid UTF-8"))?;
        collector.append_string(&result, None)?;

        Ok(())
    }

    fn evaluate_error(&self, collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncEncodeForUri {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_encode_for_uri"
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
        dispatch_unary(self, args, number_rows)
    }
}
