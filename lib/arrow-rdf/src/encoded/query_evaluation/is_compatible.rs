use crate::datatypes::RdfTerm;
use crate::encoded::dispatch_binary::{dispatch_binary, EncScalarBinaryUdf};
use crate::encoded::EncTerm;
use crate::DFResult;
use datafusion::arrow::array::BooleanBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

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
    type ArgLhs<'data> = RdfTerm<'data>;
    type ArgRhs<'data> = RdfTerm<'data>;
    type Collector = BooleanBuilder;

    fn evaluate(
        collector: &mut Self::Collector,
        lhs: &Self::ArgLhs<'_>,
        rhs: &Self::ArgRhs<'_>,
    ) -> DFResult<()> {
        let is_compatible = match (lhs, rhs) {
            (RdfTerm::BlankNode(lhs), RdfTerm::BlankNode(rhs)) => lhs == rhs,
            (RdfTerm::NamedNode(lhs), RdfTerm::NamedNode(rhs)) => lhs == rhs,
            (RdfTerm::Boolean(lhs), RdfTerm::Boolean(rhs)) => lhs == rhs,
            (RdfTerm::Numeric(lhs), RdfTerm::Numeric(rhs)) => lhs == rhs,
            (RdfTerm::SimpleLiteral(lhs), RdfTerm::SimpleLiteral(rhs)) => lhs == rhs,
            (RdfTerm::LanguageString(lhs), RdfTerm::LanguageString(rhs)) => lhs == rhs,
            (RdfTerm::TypedLiteral(lhs), RdfTerm::TypedLiteral(rhs)) => lhs == rhs,
            _ => false,
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
