use crate::encoded::{EncRdfTermBuilder, EncTerm, EncTermField};
use crate::{as_rdf_term_array, DFResult};
use datafusion::arrow;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::internal_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct EncNot {
    signature: Signature,
}

impl EncNot {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for EncNot {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_not"
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
        if args.len() != 1 {
            return internal_err!("Unexpected numer of arguments in enc_not.");
        }

        let input = args[0].to_array(number_rows)?;
        let terms = as_rdf_term_array(&input)?;
        let boolean_array = terms.child(EncTermField::Boolean.type_id()).as_boolean();

        if boolean_array.len() != number_rows {
            return internal_err!("Unexpected number of boolean elements in enc_not.");
        }

        // TODO optimize
        let mut builder = EncRdfTermBuilder::new();
        let results = arrow::compute::not(boolean_array)?;

        for result in results.iter() {
            builder.append_boolean(result.expect("Result cannot be null"))?
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()?)))
    }
}
