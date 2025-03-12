use crate::datatypes::{RdfSimpleLiteral, RdfStringLiteral};
use crate::encoded::dispatch_binary::{dispatch_binary, EncScalarBinaryUdf};
use crate::encoded::dispatch_ternary::{dispatch_ternary, EncScalarTernaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use regex::{Regex, RegexBuilder};
use std::any::Any;
use std::borrow::Cow;
use datafusion::common::exec_err;

#[derive(Debug)]
pub struct EncRegex {
    signature: Signature,
}

// TODO: Support pre-compiled regex if not a variable

impl EncRegex {
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

impl EncScalarBinaryUdf for EncRegex {
    type ArgLhs<'data> = RdfStringLiteral<'data>;
    type ArgRhs<'data> = RdfSimpleLiteral<'data>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(
        collector: &mut Self::Collector,
        text: &Self::ArgLhs<'_>,
        pattern: &Self::ArgRhs<'_>,
    ) -> DFResult<()> {
        let Some(regex) = compile_pattern(&pattern.value, None) else {
            collector.append_null()?;
            return Ok(());
        };

        collector.append_boolean(regex.is_match(&text.0))?;
        Ok(())
    }

    fn evaluate_error(collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl EncScalarTernaryUdf for EncRegex {
    type Arg0<'data> = RdfStringLiteral<'data>;
    type Arg1<'data> = RdfSimpleLiteral<'data>;
    type Arg2<'data> = RdfSimpleLiteral<'data>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(
        collector: &mut Self::Collector,
        text: &Self::Arg0<'_>,
        pattern: &Self::Arg1<'_>,
        flags: &Self::Arg2<'_>,
    ) -> DFResult<()> {
        let Some(regex) = compile_pattern(&pattern.value, Some(flags.value)) else {
            collector.append_null()?;
            return Ok(());
        };

        collector.append_boolean(regex.is_match(&text.0))?;
        Ok(())
    }

    fn evaluate_error(collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncRegex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_regex"
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
            2 => dispatch_binary::<EncRegex>(args, number_rows),
            3 => dispatch_ternary::<EncRegex>(args, number_rows),
            _ => exec_err!("Unexpected number of arguments"),
        }
    }
}

fn compile_pattern(pattern: &str, flags: Option<&str>) -> Option<Regex> {
    const REGEX_SIZE_LIMIT: usize = 1_000_000;

    let mut pattern = Cow::Borrowed(pattern);
    let flags = flags.unwrap_or_default();
    if flags.contains('q') {
        pattern = regex::escape(&pattern).into();
    }
    let mut regex_builder = RegexBuilder::new(&pattern);
    regex_builder.size_limit(REGEX_SIZE_LIMIT);
    for flag in flags.chars() {
        match flag {
            's' => {
                regex_builder.dot_matches_new_line(true);
            }
            'm' => {
                regex_builder.multi_line(true);
            }
            'i' => {
                regex_builder.case_insensitive(true);
            }
            'x' => {
                regex_builder.ignore_whitespace(true);
            }
            'q' => (),        // Already supported
            _ => return None, // invalid option
        }
    }
    regex_builder.build().ok()
}
