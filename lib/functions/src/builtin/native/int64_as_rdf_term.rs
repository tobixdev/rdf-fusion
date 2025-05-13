use crate::DFResult;
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::cast::as_int64_array;
use datafusion::common::exec_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use rdf_fusion_encoding::typed_value::{TypedValueArrayBuilder, TypedValueEncoding};
use rdf_fusion_encoding::TermEncoding;
use std::any::Any;

#[derive(Debug)]
pub struct EncInt64AsRdfTerm {
    signature: Signature,
}

impl EncInt64AsRdfTerm {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Int64]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for EncInt64AsRdfTerm {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_int64_as_rdf_term"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(TypedValueEncoding::data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("Unexpected number of arguments");
        }

        // Performance could be optimized here
        let arg = args.args[0].to_array(args.number_rows)?;
        let arg = as_int64_array(&arg)?;
        let mut builder = TypedValueArrayBuilder::default();
        for i in 0..args.number_rows {
            if arg.is_null(i) {
                builder.append_null()?;
            } else {
                builder.append_integer(arg.value(i).into())?;
            }
        }

        Ok(ColumnarValue::Array(builder.finish()))
    }
}
