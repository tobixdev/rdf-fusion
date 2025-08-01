use datafusion::arrow::array::{Array, BooleanBuilder, as_boolean_array};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_common::DFResult;
use std::any::Any;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::ops::Not;
use std::sync::Arc;

pub fn sparql_or() -> Arc<ScalarUDF> {
    let udf_impl = SparqlOr::new();
    Arc::new(ScalarUDF::new_from_impl(udf_impl))
}

#[derive(Debug)]
pub struct SparqlOr {
    name: String,
    signature: Signature,
}

impl SparqlOr {
    pub fn new() -> Self {
        Self {
            name: BuiltinName::Or.to_string(),
            signature: Signature::new(
                TypeSignature::Exact(vec![DataType::Boolean, DataType::Boolean]),
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for SparqlOr {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparqlOr {
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
                (Some(lhs), Some(rhs)) => Some(lhs || rhs),
                (Some(true), _) | (_, Some(true)) => Some(true),
                _ => None,
            };
            builder.append_option(result)
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }

    fn hash_value(&self) -> u64 {
        // Remove once https://github.com/apache/datafusion/pull/16977 is in release
        let hasher = &mut DefaultHasher::new();
        self.as_any().type_id().hash(hasher);
        hasher.finish()
    }
}
