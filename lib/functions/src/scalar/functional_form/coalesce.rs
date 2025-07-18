use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_n_ary_typed_value;
use crate::scalar::{NAryArgs, ScalarSparqlOp};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::ThinError;

#[derive(Debug)]
pub struct CoalesceSparqlOp;

impl Default for CoalesceSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CoalesceSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Coalesce);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for CoalesceSparqlOp {
    type Args<TEncoding: TermEncoding> = NAryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn return_type(&self, _input_encoding: Option<EncodingName>) -> DFResult<DataType> {
        Ok(TypedValueEncoding::data_type())
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn Fn(Self::Args<TypedValueEncoding>) -> DFResult<ColumnarValue>>> {
        Some(Box::new(|NAryArgs(args, number_rows)| {
            dispatch_n_ary_typed_value(
                &args,
                number_rows,
                |args| args.first().copied().ok_or(ThinError::Expected),
                |args| {
                    args.iter()
                        .find_map(|arg| arg.ok())
                        .ok_or(ThinError::Expected)
                },
            )
        }))
    }
}
