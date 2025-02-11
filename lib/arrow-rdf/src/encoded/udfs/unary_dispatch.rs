use crate::encoded::cast::{
    cast_bool, cast_f32, cast_f32_arr, cast_f64, cast_f64_arr, cast_i32, cast_i32_arr, cast_i64,
    cast_i64_arr, cast_str, cast_str_arr, cast_typed_literal, cast_typed_literal_array,
};
use crate::encoded::udfs::result_collector::ResultCollector;
use crate::encoded::EncTermField;
use crate::{as_rdf_term_array, DFResult};
use datafusion::arrow::array::Array;
use datafusion::common::{exec_err, not_impl_err, DataFusionError, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;

pub trait EncScalarUnaryUdf {
    type Collector: ResultCollector;

    fn supports_named_node() -> bool {
        false
    }
    fn supports_blank_node() -> bool {
        false
    }
    fn supports_numeric() -> bool {
        false
    }
    fn supports_boolean() -> bool {
        false
    }
    fn supports_string() -> bool {
        false
    }
    fn supports_date_time() -> bool {
        false
    }
    fn supports_simple_literal() -> bool {
        false
    }
    fn supports_typed_literal() -> bool {
        false
    }

    #[allow(unused_variables)]
    fn eval_named_node(collector: &mut Self::Collector, value: &str) -> DFResult<()> {
        panic!("eval_named_node")
    }

    #[allow(unused_variables)]
    fn eval_blank_node(collector: &mut Self::Collector, value: &str) -> DFResult<()> {
        panic!("eval_blank_node")
    }

    #[allow(unused_variables)]
    fn eval_numeric_i32(collector: &mut Self::Collector, value: i32) -> DFResult<()> {
        panic!("eval_numeric_i32")
    }

    #[allow(unused_variables)]
    fn eval_numeric_i64(collector: &mut Self::Collector, value: i64) -> DFResult<()> {
        panic!("eval_numeric_i64")
    }

    #[allow(unused_variables)]
    fn eval_numeric_f32(collector: &mut Self::Collector, value: f32) -> DFResult<()> {
        panic!("eval_numeric_f32")
    }

    #[allow(unused_variables)]
    fn eval_numeric_f64(collector: &mut Self::Collector, value: f64) -> DFResult<()> {
        panic!("eval_numeric_f64")
    }

    #[allow(unused_variables)]
    fn eval_boolean(collector: &mut Self::Collector, value: bool) -> DFResult<()> {
        panic!("eval_boolean")
    }

    #[allow(unused_variables)]
    fn eval_string(collector: &mut Self::Collector, value: &str) -> DFResult<()> {
        panic!("eval_string")
    }

    #[allow(unused_variables)]
    fn eval_simple_literal(collector: &mut Self::Collector, value: &str) -> DFResult<()> {
        panic!("eval_simple_literal")
    }

    #[allow(unused_variables)]
    fn eval_typed_literal(
        collector: &mut Self::Collector,
        value: &str,
        value_type: &str,
    ) -> DFResult<()> {
        panic!("eval_typed_literal")
    }

    fn eval_null(collector: &mut Self::Collector) -> DFResult<()>;

    fn eval_incompatible(collector: &mut Self::Collector) -> DFResult<()>;
}

pub fn dispatch_unary<TUdf>(
    args: &[ColumnarValue],
    _number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarUnaryUdf,
{
    match args {
        [ColumnarValue::Array(lhs)] => dispatch_unary_array::<TUdf>(lhs.clone()),
        [ColumnarValue::Scalar(lhs)] => dispatch_unary_scalar::<TUdf>(lhs.clone()),
        _ => Err(DataFusionError::Execution(String::from(
            "Unexpected type combination.",
        ))),
    }
}

fn dispatch_unary_scalar<TUdf>(value: ScalarValue) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarUnaryUdf,
{
    let ScalarValue::Union(Some((value_type_id, value)), _, _) = value else {
        return exec_err!("Unexpected type for scalar.");
    };
    let term_field = EncTermField::try_from(value_type_id).unwrap();

    let mut collector = TUdf::Collector::new();

    if value.is_null() {
        TUdf::eval_null(&mut collector)?;
        return Ok(collector.finish_columnar_value()?);
    }

    match term_field {
        EncTermField::NamedNode if TUdf::supports_named_node() => {
            let value = cast_str(&value);
            TUdf::eval_named_node(&mut collector, value)?;
        }
        EncTermField::BlankNode if TUdf::supports_blank_node() => {
            let value = cast_str(&value);
            TUdf::eval_blank_node(&mut collector, value)?;
        }
        EncTermField::Boolean if TUdf::supports_boolean() => {
            let value = cast_bool(&value);
            TUdf::eval_boolean(&mut collector, value)?;
        }
        EncTermField::Int if TUdf::supports_numeric() => {
            let value = cast_i32(&value);
            TUdf::eval_numeric_i32(&mut collector, value)?;
        }
        EncTermField::Integer if TUdf::supports_numeric() => {
            let value = cast_i64(&value);
            TUdf::eval_numeric_i64(&mut collector, value)?;
        }
        EncTermField::Float32 if TUdf::supports_numeric() => {
            let value = cast_f32(&value);
            TUdf::eval_numeric_f32(&mut collector, value)?;
        }
        EncTermField::Float64 if TUdf::supports_numeric() => {
            let value = cast_f64(&value);
            TUdf::eval_numeric_f64(&mut collector, value)?;
        }
        EncTermField::String if TUdf::supports_string() => {
            todo!()
        }
        EncTermField::TypedLiteral if TUdf::supports_typed_literal() => {
            let (value, value_type) = cast_typed_literal(term_field, &value)?;
            TUdf::eval_typed_literal(&mut collector, &value, &value_type)?;
        }
        _ => TUdf::eval_incompatible(&mut collector)?,
    }

    collector.finish_columnar_value()
}

fn dispatch_unary_array<TUdf>(values: Arc<dyn Array>) -> DFResult<ColumnarValue>
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
                let value = cast_str_arr(values, value_type, *offset as usize);
                TUdf::eval_named_node(&mut collector, value)?;
            }
            EncTermField::BlankNode => {
                let value = cast_str_arr(values, value_type, *offset as usize);
                TUdf::eval_blank_node(&mut collector, value)?;
            }
            EncTermField::Int => {
                let value = cast_i32_arr(values, value_type, *offset as usize);
                TUdf::eval_numeric_i32(&mut collector, value)?;
            }
            EncTermField::Integer => {
                let value = cast_i64_arr(values, value_type, *offset as usize);
                TUdf::eval_numeric_i64(&mut collector, value)?;
            }
            EncTermField::Float32 => {
                let value = cast_f32_arr(values, value_type, *offset as usize);
                TUdf::eval_numeric_f32(&mut collector, value)?;
            }
            EncTermField::Float64 => {
                let value = cast_f64_arr(values, value_type, *offset as usize);
                TUdf::eval_numeric_f64(&mut collector, value)?;
            }
            EncTermField::TypedLiteral => {
                let (value, value_type) =
                    cast_typed_literal_array(values, value_type, *offset as usize);
                TUdf::eval_typed_literal(&mut collector, &value, &value_type)?;
            }
            t => return not_impl_err!("dispatch_unary_array for {t:?}"),
        }
    }
    collector.finish_columnar_value()
}
