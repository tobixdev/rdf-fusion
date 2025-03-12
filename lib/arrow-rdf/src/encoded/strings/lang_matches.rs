use crate::datatypes::RdfSimpleLiteral;
use crate::encoded::dispatch_binary::{dispatch_binary, EncScalarBinaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

#[derive(Debug)]
pub struct EncLangMatches {
    signature: Signature,
}

impl EncLangMatches {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type(); 2]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarBinaryUdf for EncLangMatches {
    type ArgLhs<'lhs> = RdfSimpleLiteral<'lhs>;
    type ArgRhs<'lhs> = RdfSimpleLiteral<'lhs>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(
        collector: &mut Self::Collector,
        language_tag: &Self::ArgLhs<'_>,
        language_range: &Self::ArgRhs<'_>,
    ) -> DFResult<()> {
        let matches = if &*language_range.value == "*" {
            !language_tag.value.is_empty()
        } else {
            !ZipLongest::new(language_range.value.split('-'), language_tag.value.split('-'))
                .any(|parts| match parts {
                    (Some(range_subtag), Some(language_subtag)) => {
                        !range_subtag.eq_ignore_ascii_case(language_subtag)
                    }
                    (Some(_), None) => true,
                    (None, _) => false,
                })
        };
        collector.append_boolean(matches)?;
        Ok(())
    }

    fn evaluate_error(collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncLangMatches {
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
        dispatch_binary::<EncLangMatches>(args, number_rows)
    }
}


struct ZipLongest<T1, T2, I1: Iterator<Item = T1>, I2: Iterator<Item = T2>> {
    a: I1,
    b: I2,
}

impl<T1, T2, I1: Iterator<Item = T1>, I2: Iterator<Item = T2>> ZipLongest<T1, T2, I1, I2> {
    fn new(a: I1, b: I2) -> Self {
        Self { a, b }
    }
}

impl<T1, T2, I1: Iterator<Item = T1>, I2: Iterator<Item = T2>> Iterator
for ZipLongest<T1, T2, I1, I2>
{
    type Item = (Option<T1>, Option<T2>);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.a.next(), self.b.next()) {
            (None, None) => None,
            r => Some(r),
        }
    }
}