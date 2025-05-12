use crate::builtin::{BuiltinName, GraphFusionUdfFactory};
use crate::{DFResult, FunctionName};
use datafusion::arrow::array::{as_boolean_array, Array, BooleanBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use graphfusion_encoding::EncodingName;
use graphfusion_model::Term;
use std::any::Any;
use std::collections::HashMap;
use std::ops::Not;
use std::sync::Arc;

#[derive(Debug)]
pub struct SparqlOrFactory;

impl GraphFusionUdfFactory for SparqlOrFactory {
    fn name(&self) -> FunctionName {
        FunctionName::Builtin(BuiltinName::And)
    }

    fn encoding(&self) -> Vec<EncodingName> {
        vec![EncodingName::TypedValue]
    }

    fn create_with_args(&self, _constant_args: HashMap<String, Term>) -> DFResult<Arc<ScalarUDF>> {
        let udf = ScalarUDF::new_from_impl(SparqlOr::new());
        Ok(Arc::new(udf))
    }
}

#[derive(Debug)]
pub struct SparqlOr {
    signature: Signature,
}

impl SparqlOr {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Boolean, DataType::Boolean]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparqlOr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "or"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs<'_>,
    ) -> datafusion::common::Result<ColumnarValue> {
        let lhs = args.args[0].to_array(args.number_rows)?;
        let rhs = args.args[1].to_array(args.number_rows)?;

        let lhs = as_boolean_array(&lhs);
        let rhs = as_boolean_array(&rhs);

        let mut builder = BooleanBuilder::with_capacity(args.number_rows);
        for i in 0..args.number_rows {
            let lhs = lhs.is_null(i).not().then(|| lhs.value(i));
            let rhs = rhs.is_null(i).not().then(|| rhs.value(i));

            let result = match (lhs, rhs) {
                (Some(lhs), Some(rhs)) => Some(lhs || rhs),
                (Some(true), _) | (_, Some(true)) => Some(true),
                _ => None,
            };
            builder.append_option(result)
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}
