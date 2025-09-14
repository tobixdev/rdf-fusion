use datafusion::arrow::array::{Array, BooleanArray, BooleanBuilder, make_comparator};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::compute::kernels::cmp::eq;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::{EncodingName, RdfFusionEncodings};
use std::any::Any;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub fn is_compatible(encodings: &RdfFusionEncodings) -> ScalarUDF {
    let udf_impl = IsCompatible::new(encodings);
    ScalarUDF::new_from_impl(udf_impl)
}

#[derive(Debug, Eq)]
struct IsCompatible {
    name: String,
    signature: Signature,
}

impl IsCompatible {
    pub fn new(encodings: &RdfFusionEncodings) -> Self {
        Self {
            name: BuiltinName::IsCompatible.to_string(),
            signature: Signature::new(
                TypeSignature::Uniform(
                    2,
                    encodings.get_data_types(&[
                        EncodingName::PlainTerm,
                        EncodingName::ObjectId,
                    ]),
                ),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for IsCompatible {
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match TryInto::<[_; 2]>::try_into(args.args) {
            Ok([ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)]) => {
                invoke_array_array(args.number_rows, lhs.as_ref(), rhs.as_ref())
            }
            Ok([ColumnarValue::Scalar(lhs), ColumnarValue::Array(rhs)]) => {
                invoke_scalar_array(args.number_rows, &lhs, rhs.as_ref())
            }
            Ok([ColumnarValue::Array(lhs), ColumnarValue::Scalar(rhs)]) => {
                // Commutative operation
                invoke_scalar_array(args.number_rows, &rhs, lhs.as_ref())
            }
            Ok([ColumnarValue::Scalar(lhs), ColumnarValue::Scalar(rhs)]) => {
                Ok(invoke_scalar_scalar(&lhs, &rhs))
            }
            _ => exec_err!("Invalid arguments for IsCompatible"),
        }
    }
}

impl Hash for IsCompatible {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_any().type_id().hash(state);
    }
}

impl PartialEq for IsCompatible {
    fn eq(&self, other: &Self) -> bool {
        self.as_any().type_id() == other.as_any().type_id()
            && self.signature.eq(&other.signature)
    }
}

pub(crate) fn invoke_array_array(
    number_rows: usize,
    lhs: &dyn Array,
    rhs: &dyn Array,
) -> DFResult<ColumnarValue> {
    let mut eq_res = invoke_eq_array(number_rows, lhs, rhs)?;

    if eq_res.null_count() > 0 {
        eq_res = fill_nulls(&eq_res, true);
    }

    Ok(ColumnarValue::Array(Arc::new(eq_res)))
}

pub(crate) fn invoke_scalar_array(
    number_rows: usize,
    lhs: &ScalarValue,
    rhs: &dyn Array,
) -> DFResult<ColumnarValue> {
    if lhs.is_null() {
        return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))));
    }

    let eq_res = invoke_eq_array_scalar(number_rows, rhs, lhs)?;
    if eq_res.null_count() > 0 {
        let result = fill_nulls(&eq_res, true);
        Ok(ColumnarValue::Array(Arc::new(result)))
    } else {
        Ok(ColumnarValue::Array(Arc::new(eq_res)))
    }
}

pub(crate) fn invoke_scalar_scalar(
    lhs: &ScalarValue,
    rhs: &ScalarValue,
) -> ColumnarValue {
    ColumnarValue::Scalar(ScalarValue::Boolean(Some(
        lhs.is_null() || rhs.is_null() || lhs == rhs,
    )))
}

fn invoke_eq_array(
    number_rows: usize,
    lhs: &dyn Array,
    rhs: &dyn Array,
) -> DFResult<BooleanArray> {
    let data_type = lhs.data_type();
    if data_type.is_nested() {
        let comparator = make_comparator(lhs, rhs, SortOptions::default())?;
        let result = (0..number_rows)
            .map(|i| {
                Some(
                    lhs.is_null(i)
                        || rhs.is_null(i)
                        || comparator(i, i) == Ordering::Equal,
                )
            })
            .collect::<BooleanArray>();
        Ok(result)
    } else {
        Ok(eq(&lhs, &rhs)?)
    }
}

fn invoke_eq_array_scalar(
    number_rows: usize,
    lhs: &dyn Array,
    rhs: &ScalarValue,
) -> DFResult<BooleanArray> {
    let rhs = rhs.to_array_of_size(number_rows)?;
    invoke_eq_array(number_rows, lhs, &rhs)
}

fn fill_nulls(bool_array: &BooleanArray, fill_value: bool) -> BooleanArray {
    let mut builder = BooleanBuilder::with_capacity(bool_array.len());

    for i in 0..bool_array.len() {
        if bool_array.is_null(i) {
            builder.append_value(fill_value);
        } else {
            builder.append_value(bool_array.value(i));
        }
    }

    builder.finish()
}
