use crate::datatypes::RdfTerm;
use crate::encoded::dispatch_binary::{dispatch_binary, EncScalarBinaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use std::cmp::Ordering;

#[derive(Debug)]
pub struct EncEq {
    signature: Signature,
}

impl EncEq {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type(), EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarBinaryUdf for EncEq {
    type ArgLhs<'lhs> = RdfTerm<'lhs>;
    type ArgRhs<'rhs> = RdfTerm<'rhs>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(
        collector: &mut Self::Collector,
        lhs: &Self::ArgLhs<'_>,
        rhs: &Self::ArgRhs<'_>,
    ) -> DFResult<()> {
        let result = match (lhs, rhs) {
            (RdfTerm::NamedNode(l), RdfTerm::NamedNode(r)) => l == r,
            (RdfTerm::BlankNode(l), RdfTerm::BlankNode(r)) => l == r,
            (RdfTerm::Boolean(l), RdfTerm::Boolean(r)) => l == r,
            (RdfTerm::Numeric(l), RdfTerm::Numeric(r)) => l.cmp(r) == Ordering::Equal,
            (RdfTerm::SimpleLiteral(l), RdfTerm::SimpleLiteral(r)) => l == r,
            (RdfTerm::LanguageString(l), RdfTerm::LanguageString(r)) => l == r,
            (RdfTerm::TypedLiteral(l), RdfTerm::TypedLiteral(r)) => l == r,
            _ => false,
        };
        collector.append_boolean(result)?;
        Ok(())
    }

    fn evaluate_error(collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncEq {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_eq"
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
        // Maybe we can improve this as we don't need the casting for same term (I think)
        dispatch_binary::<Self>(args, number_rows)
    }
}
