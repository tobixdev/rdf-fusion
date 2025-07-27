use datafusion::arrow::array::{Array, BooleanBuilder, as_boolean_array};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_common::DFResult;
use std::any::Any;
use std::ops::Not;
use std::sync::Arc;

pub fn sparql_and() -> Arc<ScalarUDF> {
    let udf_impl = SparqlAnd::new();
    Arc::new(ScalarUDF::new_from_impl(udf_impl))
}

#[derive(Debug)]
struct SparqlAnd {
    name: String,
    signature: Signature,
}

impl SparqlAnd {
    pub fn new() -> Self {
        Self {
            name: BuiltinName::And.to_string(),
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
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
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
