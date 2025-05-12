use crate::{DFResult, FunctionName};
use datafusion::arrow::array::{as_boolean_array, Array, BooleanBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use std::any::Any;
use std::collections::HashMap;
use std::ops::Not;
use std::sync::Arc;
use graphfusion_encoding::EncodingName;
use graphfusion_model::Term;
use crate::builtin::{BuiltinName, GraphFusionUdfFactory};

#[derive(Debug)]
pub struct SparqlAndFactory;

impl GraphFusionUdfFactory for SparqlAndFactory {
    fn name(&self) -> FunctionName {
        FunctionName::Builtin(BuiltinName::And)
    }

    fn encoding(&self) -> Vec<EncodingName> {
        vec![EncodingName::TypedValue]
    }

    fn create_with_args(&self, _constant_args: HashMap<String, Term>) -> DFResult<Arc<ScalarUDF>> {
        let udf = ScalarUDF::new_from_impl(SparqlAnd::new());
        Ok(Arc::new(udf))
    }
}

#[derive(Debug)]
pub struct SparqlAnd {
    signature: Signature,
}

impl SparqlAnd {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Boolean, DataType::Boolean]),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparqlAnd {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "and"
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
                (Some(lhs), Some(rhs)) => Some(lhs && rhs),
                (Some(false), _) | (_, Some(false)) => Some(false),
                _ => None,
            };
            builder.append_option(result)
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}
