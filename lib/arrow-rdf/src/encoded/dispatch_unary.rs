use crate::encoded::cast::{cast_bool, cast_bool_arr, cast_decimal, cast_decimal_arr, cast_f32, cast_f32_arr, cast_f64, cast_f64_arr, cast_i32, cast_i32_arr, cast_i64, cast_i64_arr, cast_str, cast_str_arr, cast_typed_literal, cast_typed_literal_array};
use crate::encoded::EncTermField;
use crate::result_collector::ResultCollector;
use crate::{as_rdf_term_array, DFResult};
use datafusion::arrow::array::Array;
use datafusion::common::{exec_err, DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;

pub(crate) trait EncScalarUnaryUdf {
    type Collector: ResultCollector;

    fn eval_named_node(&self, collector: &mut Self::Collector, value: &str) -> DFResult<()>;

    fn eval_blank_node(&self, collector: &mut Self::Collector, value: &str) -> DFResult<()>;

    fn eval_numeric_i32(&self, collector: &mut Self::Collector, value: i32) -> DFResult<()>;

    fn eval_numeric_i64(&self, collector: &mut Self::Collector, value: i64) -> DFResult<()>;

    fn eval_numeric_f32(&self, collector: &mut Self::Collector, value: f32) -> DFResult<()>;

    fn eval_numeric_f64(&self, collector: &mut Self::Collector, value: f64) -> DFResult<()>;

    fn eval_numeric_decimal(&self, collector: &mut Self::Collector, value: i128) -> DFResult<()>;

    fn eval_boolean(&self, collector: &mut Self::Collector, value: bool) -> DFResult<()>;

    fn eval_string(
        &self,
        collector: &mut Self::Collector,
        value: &str,
        lang: Option<&str>,
    ) -> DFResult<()>;

    fn eval_typed_literal(
        &self,
        collector: &mut Self::Collector,
        value: &str,
        value_type: &str,
    ) -> DFResult<()>;

    fn eval_null(&self, collector: &mut Self::Collector) -> DFResult<()>;
}

pub fn dispatch_unary<TUdf>(
    udf: &TUdf,
    args: &[ColumnarValue],
    _number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarUnaryUdf,
{
    match args {
        [ColumnarValue::Array(lhs)] => dispatch_unary_array(udf, lhs.clone()),
        [ColumnarValue::Scalar(lhs)] => dispatch_unary_scalar(udf, lhs.clone()),
        _ => Err(DataFusionError::Execution(String::from(
            "Unexpected type combination.",
        ))),
    }
}

fn dispatch_unary_scalar<TUdf>(udf: &TUdf, value: ScalarValue) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarUnaryUdf,
{
    let ScalarValue::Union(Some((value_type_id, value)), _, _) = value else {
        return exec_err!("Unexpected type for scalar.");
    };
    let term_field = EncTermField::try_from(value_type_id).unwrap();

    let mut collector = TUdf::Collector::new();

    if value.is_null() {
        udf.eval_null(&mut collector)?;
        return Ok(collector.finish_columnar_value()?);
    }

    match term_field {
        EncTermField::NamedNode => {
            let (value, _) = cast_str(&value);
            udf.eval_named_node(&mut collector, value)?;
        }
        EncTermField::BlankNode => {
            let (value, _) = cast_str(&value);
            udf.eval_blank_node(&mut collector, value)?;
        }
        EncTermField::Boolean => {
            let value = cast_bool(&value);
            udf.eval_boolean(&mut collector, value)?;
        }
        EncTermField::Int => {
            let value = cast_i32(&value);
            udf.eval_numeric_i32(&mut collector, value)?;
        }
        EncTermField::Integer => {
            let value = cast_i64(&value);
            udf.eval_numeric_i64(&mut collector, value)?;
        }
        EncTermField::Float32 => {
            let value = cast_f32(&value);
            udf.eval_numeric_f32(&mut collector, value)?;
        }
        EncTermField::Float64 => {
            let value = cast_f64(&value);
            udf.eval_numeric_f64(&mut collector, value)?;
        }
        EncTermField::String => {
            let (value, lang) = cast_str(&value);
            udf.eval_string(&mut collector, value, lang)?;
        }
        EncTermField::TypedLiteral => {
            let (value, value_type) = cast_typed_literal(term_field, &value)?;
            udf.eval_typed_literal(&mut collector, &value, &value_type)?;
        }
        EncTermField::Decimal => {
            let value = cast_decimal(&value);
            udf.eval_numeric_decimal(&mut collector, value)?;
        }
    }

    collector.finish_columnar_value()
}

fn dispatch_unary_array<TUdf>(udf: &TUdf, values: Arc<dyn Array>) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarUnaryUdf,
{
    let values = as_rdf_term_array(&values).expect("RDF term array expected");
    let type_offset_paris = values
        .type_ids()
        .iter()
        .map(|tid| EncTermField::try_from(*tid))
        .zip(values.offsets().expect("Always Dense"));

    let mut collector = TUdf::Collector::new();
    for (value_type, offset) in type_offset_paris {
        let value_type = value_type?;
        match value_type {
            EncTermField::NamedNode => {
                let (value, _) = cast_str_arr(values, value_type, *offset as usize);
                udf.eval_named_node(&mut collector, value)?;
            }
            EncTermField::BlankNode => {
                let (value, _) = cast_str_arr(values, value_type, *offset as usize);
                udf.eval_blank_node(&mut collector, value)?;
            }
            EncTermField::Boolean => {
                let value = cast_bool_arr(values, value_type, *offset as usize);
                udf.eval_boolean(&mut collector, value)?;
            }
            EncTermField::Int => {
                let value = cast_i32_arr(values, value_type, *offset as usize);
                udf.eval_numeric_i32(&mut collector, value)?;
            }
            EncTermField::Integer => {
                let value = cast_i64_arr(values, value_type, *offset as usize);
                udf.eval_numeric_i64(&mut collector, value)?;
            }
            EncTermField::Float32 => {
                let value = cast_f32_arr(values, value_type, *offset as usize);
                udf.eval_numeric_f32(&mut collector, value)?;
            }
            EncTermField::Float64 => {
                let value = cast_f64_arr(values, value_type, *offset as usize);
                udf.eval_numeric_f64(&mut collector, value)?;
            }
            EncTermField::TypedLiteral => {
                let (value, value_type) =
                    cast_typed_literal_array(values, value_type, *offset as usize);
                udf.eval_typed_literal(&mut collector, &value, &value_type)?;
            }
            EncTermField::String => {
                let (value, lang) = cast_str_arr(values, value_type, *offset as usize);
                udf.eval_string(&mut collector, value, lang)?;
            }
            EncTermField::Decimal => {
                let value = cast_decimal_arr(values, value_type, *offset as usize);
                udf.eval_numeric_decimal(&mut collector, value)?;
            }
        }
    }
    collector.finish_columnar_value()
}
