use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_datafusion_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use rdf_fusion_common::DFResult;
use std::any::Any;
use std::collections::{HashMap, HashSet};

#[derive(Debug, PartialEq, Eq, Hash)]
struct UDFKey(usize, DataType);

/// A dynamic user-defined function that dispatches to other UDFs based on arity and data type.
///
/// This is used to implement SPARQL functions that can have multiple signatures,
/// for example, `SUBSTR` which can have 2 or 3 arguments.
///
/// In the future we plan to have a better concept handling this issue.
#[derive(Debug)]
pub struct DynamicRdfFusionUdf {
    /// The name of the function.
    name: String,
    /// The signature of the function.
    signature: Signature,
    /// A mapping from (arity, data type) to the actual UDF.
    arity_to_udf_mapping: HashMap<UDFKey, ScalarUDF>,
}

impl DynamicRdfFusionUdf {
    /// Creates a new [DynamicRdfFusionUdf].
    pub fn try_new(name: &FunctionName, inner: &[ScalarUDF]) -> DFResult<Self> {
        validate_inner_udfs(name, inner)?;

        let udf_mapping = build_udf_mapping(inner)?;
        let volatility = get_safe_volatility(inner);
        let type_signature = inner
            .iter()
            .map(|udf| udf.signature().type_signature.clone())
            .collect::<HashSet<_>>();
        let type_signature = TypeSignature::OneOf(type_signature.into_iter().collect());

        Ok(Self {
            name: name.to_string(),
            signature: Signature::new(type_signature, volatility),
            arity_to_udf_mapping: udf_mapping,
        })
    }
}

impl ScalarUDFImpl for DynamicRdfFusionUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        let arity = arg_types.len();
        let data_type = if arity == 0 {
            DataType::Null
        } else {
            arg_types[0].clone()
        };
        let key = UDFKey(arity, data_type);

        let inner = self
            .arity_to_udf_mapping
            .get(&key)
            .ok_or(plan_datafusion_err!(
                "Arity of the given arg_types do not match any inner UDFs."
            ))?;
        inner.return_type(arg_types)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        let arity = args.args.len();
        let data_type = if arity == 0 {
            DataType::Null
        } else {
            args.args[0].data_type().clone()
        };
        let key = UDFKey(arity, data_type);

        let inner = self
            .arity_to_udf_mapping
            .get(&key)
            .ok_or(plan_datafusion_err!(
                "Arity of the given args do not match any inner UDFs."
            ))?;
        inner.invoke_with_args(args)
    }
}

/// Validates that all inner UDFs have the same name and that the name matches the dynamic UDF.
fn validate_inner_udfs(name: &FunctionName, inner: &[ScalarUDF]) -> DFResult<()> {
    if inner.is_empty() {
        return plan_err!("No UDFs provided for multi-arity SPARQL function.");
    }

    let names = inner.iter().map(ScalarUDF::name).collect::<HashSet<_>>();
    if names.len() != 1 {
        return plan_err!("All UDFs for multi-arity SPARQL function must have the same name.");
    }

    let inner_name = names.into_iter().next().unwrap();
    if name.to_string() != inner_name {
        return plan_err!("The names of the inner UDFs for multi-arity SPARQL function must match the name of the multi-arity SPARQL function.");
    }

    Ok(())
}

/// Builds a mapping from (arity, data type) to UDF.
fn build_udf_mapping(inner: &[ScalarUDF]) -> DFResult<HashMap<UDFKey, ScalarUDF>> {
    let keys_per_udf = inner
        .iter()
        .map(|udf| match &udf.signature().type_signature {
            TypeSignature::Uniform(arity, data_type) => Ok(data_type
                .iter()
                .map(|dt| (UDFKey(*arity, dt.clone()), udf.clone()))
                .collect::<Vec<_>>()),
            TypeSignature::Nullary => Ok(vec![(UDFKey(0, DataType::Null), udf.clone())]),
            _ => {
                plan_err!("MultiArity SparqlUdf only supported for Nullary and Uniform signatures")
            }
        })
        .collect::<DFResult<Vec<_>>>()?;

    let mut arity_to_udf_mapping = HashMap::new();
    for (key, udf) in keys_per_udf.into_iter().flatten() {
        if arity_to_udf_mapping.contains_key(&key) {
            return plan_err!("Multiple functions with the same key (arity + data type) are not supported for dynamic SPARQL function.");
        }
        arity_to_udf_mapping.insert(key, udf);
    }

    Ok(arity_to_udf_mapping)
}

/// Returns the safest volatility of all UDFs.
///
/// The order of safety is: Volatile > Stable > Immutable.
fn get_safe_volatility(udfs: &[ScalarUDF]) -> Volatility {
    let volatilities = udfs
        .iter()
        .map(|udf| udf.signature().volatility)
        .collect::<HashSet<_>>();

    if volatilities.contains(&Volatility::Volatile) {
        return Volatility::Volatile;
    }

    if volatilities.contains(&Volatility::Stable) {
        return Volatility::Stable;
    }

    Volatility::Stable
}
