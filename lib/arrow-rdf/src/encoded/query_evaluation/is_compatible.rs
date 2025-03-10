use crate::encoded::dispatch::EncRdfTerm;
use crate::encoded::dispatch_binary::{dispatch_binary, EncScalarBinaryUdf};
use crate::encoded::EncTerm;
use crate::DFResult;
use datafusion::arrow::array::BooleanBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use crate::encoded::query_evaluation::is_compatible;

#[derive(Debug)]
pub struct EncIsCompatible {
    signature: Signature,
}

impl EncIsCompatible {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type(), EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarBinaryUdf for EncIsCompatible {
    type ArgLhs<'data> = EncRdfTerm<'data>;
    type ArgRhs<'data> = EncRdfTerm<'data>;
    type Collector = BooleanBuilder;

    fn evaluate(
        collector: &mut Self::Collector,
        lhs: &Self::ArgLhs<'_>,
        rhs: &Self::ArgRhs<'_>,
    ) -> DFResult<()> {
        let is_compatible = match (lhs, rhs) {
            (EncRdfTerm::BlankNode(lhs), EncRdfTerm::BlankNode(rhs)) => lhs == rhs,
            (EncRdfTerm::NamedNode(lhs), EncRdfTerm::NamedNode(rhs)) => lhs == rhs,
            (EncRdfTerm::Boolean(lhs), EncRdfTerm::Boolean(rhs)) => lhs == rhs,
            (EncRdfTerm::Numeric(lhs), EncRdfTerm::Numeric(rhs)) => lhs == rhs,
            (EncRdfTerm::SimpleLiteral(lhs), EncRdfTerm::SimpleLiteral(rhs)) => lhs == rhs,
            (EncRdfTerm::LanguageString(lhs), EncRdfTerm::LanguageString(rhs)) => lhs == rhs,
            (EncRdfTerm::TypedLiteral(lhs), EncRdfTerm::TypedLiteral(rhs)) => lhs == rhs,
            _ => false
        };
        collector.append_value(is_compatible);
        Ok(())
    }

    fn evaluate_error(collector: &mut Self::Collector) -> DFResult<()> {
        // At least one value is NULL, therefore the columns are compatible.
        collector.append_value(true);
        Ok(())
    }
}

impl ScalarUDFImpl for EncIsCompatible {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_is_compatible"
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
        dispatch_binary::<Self>(args, number_rows)
    }
}
