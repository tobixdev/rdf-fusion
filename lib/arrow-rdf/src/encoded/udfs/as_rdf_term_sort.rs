use crate::encoded::EncTerm;
use crate::sorting::RdfTermSort;
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::not_impl_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

#[derive(Debug)]
pub struct EncAsRdfTermSort {
    signature: Signature,
}

impl EncAsRdfTermSort {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for EncAsRdfTermSort {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_as_rdf_term_sort"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(RdfTermSort::data_type())
    }

    fn invoke_batch(
        &self,
        _args: &[ColumnarValue],
        _number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        not_impl_err!("Sorting not implemented")
    }
}
