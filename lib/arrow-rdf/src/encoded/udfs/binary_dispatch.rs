use crate::encoded::cast::{
    cast_f32, cast_f32_arr, cast_f64, cast_f64_arr, cast_i32, cast_i32_arr, cast_i64, cast_i64_arr,
    cast_str, cast_str_arr, cast_typed_literal, cast_typed_literal_array,
};
use crate::encoded::udfs::result_collector::ResultCollector;
use crate::encoded::EncTermField;
use crate::{as_rdf_term_array, DFResult};
use datafusion::arrow::array::Array;
use datafusion::common::{
    exec_err, not_impl_datafusion_err, not_impl_err, DataFusionError, ScalarValue,
};
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;

pub trait EncScalarBinaryUdf {
    type Collector: ResultCollector;

    fn supports_named_node() -> bool;
    fn supports_blank_node() -> bool;
    fn supports_numeric() -> bool;
    fn supports_boolean() -> bool;
    fn supports_string() -> bool;
    fn supports_date_time() -> bool;
    fn supports_simple_literal() -> bool;
    fn supports_typed_literal() -> bool;

    fn eval_named_node(collector: &mut Self::Collector, lhs: &str, rhs: &str) -> DFResult<()>;

    fn eval_blank_node(collector: &mut Self::Collector, lhs: &str, rhs: &str) -> DFResult<()>;

    fn eval_numeric_i32(collector: &mut Self::Collector, lhs: i32, rhs: i32) -> DFResult<()>;
    fn eval_numeric_i64(collector: &mut Self::Collector, lhs: i64, rhs: i64) -> DFResult<()>;
    fn eval_numeric_f32(collector: &mut Self::Collector, lhs: f32, rhs: f32) -> DFResult<()>;
    fn eval_numeric_f64(collector: &mut Self::Collector, lhs: f64, rhs: f64) -> DFResult<()>;

    fn eval_boolean(collector: &mut Self::Collector, lhs: bool, rhs: bool) -> DFResult<()>;

    fn eval_string(collector: &mut Self::Collector, lhs: &str, rhs: &str) -> DFResult<()>;

    fn eval_simple_literal(collector: &mut Self::Collector, lhs: &str, rhs: &str) -> DFResult<()>;

    fn eval_typed_literal(
        collector: &mut Self::Collector,
        lhs: &str,
        lhs_type: &str,
        rhs: &str,
        rhs_type: &str,
    ) -> DFResult<()>;

    fn eval_incompatible(collector: &mut Self::Collector) -> DFResult<()>;
}

pub fn dispatch_binary<TUdf>(
    args: &[ColumnarValue],
    _number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarBinaryUdf,
{
    match args {
        [ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)] => {
            dispatch_binary_array_array::<TUdf>(lhs.clone(), rhs.clone())
        }
        [ColumnarValue::Scalar(lhs), ColumnarValue::Array(rhs)] => {
            dispatch_binary_scalar_array::<TUdf>(lhs.clone(), rhs.clone())
        }
        [ColumnarValue::Array(lhs), ColumnarValue::Scalar(rhs)] => {
            dispatch_binary_array_scalar::<TUdf>(rhs.clone(), lhs.clone())
        }
        [ColumnarValue::Scalar(lhs), ColumnarValue::Scalar(rhs)] => {
            dispatch_binary_scalar_scalar::<TUdf>(lhs.clone(), rhs.clone())
        }
        _ => Err(DataFusionError::Execution(String::from(
            "Unexpected type combination.",
        ))),
    }
}

fn dispatch_binary_array_array<TUdf>(
    _lhs: Arc<dyn Array>,
    _rhs: Arc<dyn Array>,
) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarBinaryUdf,
{
    Err(not_impl_datafusion_err!("dispatch_binary_array_array"))
}

fn dispatch_binary_scalar_array<TUdf>(
    lhs: ScalarValue,
    _rhs: Arc<dyn Array>,
) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarBinaryUdf,
{
    let ScalarValue::Union(element, _, _) = lhs else {
        return exec_err!("Unexpected type for scalar.");
    };

    match element {
        None => not_impl_err!("dispatch_binary_scalar_array: None Case"),
        Some(_) => not_impl_err!("dispatch_binary_scalar_array: Some Case"),
    }
}

fn dispatch_binary_array_scalar<TUdf>(
    lhs: ScalarValue,
    rhs: Arc<dyn Array>,
) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarBinaryUdf,
{
    if lhs.is_null() {
        return not_impl_err!("dispatch_binary_array_scalar: None Case");
    }

    let (lhs_type, lhs_value) = extract_scalar_value(lhs)?;
    let rhs = as_rdf_term_array(&rhs).expect("RDF term");
    let type_offset_paris = rhs
        .type_ids()
        .iter()
        .map(|tid| EncTermField::try_from(*tid))
        .zip(rhs.offsets().expect("Always Dense"));

    let mut collector = TUdf::Collector::new();
    for (rhs_term_field, rhs_offset) in type_offset_paris {
        let rhs_term_field = rhs_term_field?;

        match decide_type::<TUdf>(lhs_type, rhs_term_field) {
            UdfTarget::NamedNode => {
                let lhs = cast_str(&lhs_value);
                let rhs = cast_str_arr(rhs, rhs_term_field, *rhs_offset as usize);
                TUdf::eval_named_node(&mut collector, lhs, rhs)?;
            }
            UdfTarget::BlankNode => {
                let lhs = cast_str(&lhs_value);
                let rhs = cast_str_arr(rhs, rhs_term_field, *rhs_offset as usize);
                TUdf::eval_blank_node(&mut collector, lhs, rhs)?;
            }
            UdfTarget::NumericI32 => {
                let lhs = cast_i32(&lhs_value);
                let rhs = cast_i32_arr(rhs, rhs_term_field, *rhs_offset as usize);
                TUdf::eval_numeric_i32(&mut collector, lhs, rhs)?;
            }
            UdfTarget::NumericI64 => {
                let lhs = cast_i64(&lhs_value);
                let rhs = cast_i64_arr(rhs, rhs_term_field, *rhs_offset as usize);
                TUdf::eval_numeric_i64(&mut collector, lhs, rhs)?;
            }
            UdfTarget::NumericF32 => {
                let lhs = cast_f32(&lhs_value);
                let rhs = cast_f32_arr(rhs, rhs_term_field, *rhs_offset as usize);
                TUdf::eval_numeric_f32(&mut collector, lhs, rhs)?;
            }
            UdfTarget::NumericF64 => {
                let lhs = cast_f64(&lhs_value);
                let rhs = cast_f64_arr(rhs, rhs_term_field, *rhs_offset as usize);
                TUdf::eval_numeric_f64(&mut collector, lhs, rhs)?;
            }
            UdfTarget::TypedLiteral => {
                let (lhs_value, lhs_type) = cast_typed_literal(lhs_type, &lhs_value)?;
                let (rhs_value, rhs_type) =
                    cast_typed_literal_array(rhs, rhs_term_field, *rhs_offset as usize);
                TUdf::eval_typed_literal(
                    &mut collector,
                    &lhs_value,
                    &lhs_type,
                    &rhs_value,
                    &rhs_type,
                )?;
            }
            UdfTarget::Incompatible => {
                TUdf::eval_incompatible(&mut collector)?;
            }
            t => return not_impl_err!("dispatch_binary_array_scalar for {t:?}"),
        }
    }
    collector.finish_columnar_value()
}

fn dispatch_binary_scalar_scalar<TUdf>(
    lhs: ScalarValue,
    rhs: ScalarValue,
) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarBinaryUdf,
{
    if lhs.is_null() != rhs.is_null() {
        return Ok(ColumnarValue::Scalar(ScalarValue::Null));
    }

    if lhs.is_null() && rhs.is_null() {
        return Ok(ColumnarValue::Scalar(ScalarValue::Null));
    }

    let (lhs_type, lhs_value) = extract_scalar_value(lhs)?;
    let (rhs_type, rhs_value) = extract_scalar_value(rhs)?;

    let cast_to = decide_type::<TUdf>(lhs_type, rhs_type);
    let mut collector = TUdf::Collector::new();
    match cast_to {
        // UdfTarget::NamedNode => {
        //     let lhs = cast_str(lhs_value);
        //     let rhs = cast_str(rhs_value);
        //     TUdf::eval_named_node(&mut collector, lhs, rhs)
        // }
        // UdfTarget::BlankNode => {
        //     let lhs = cast_str(lhs_value);
        //     let rhs = cast_str(rhs_value);
        //     TUdf::eval_blank_node(&mut collector, lhs, rhs)
        // }
        UdfTarget::NumericI32 => {
            let lhs = cast_i32(&lhs_value);
            let rhs = cast_i32(&rhs_value);
            TUdf::eval_numeric_i32(&mut collector, lhs, rhs)
        }
        UdfTarget::NumericI64 => {
            let lhs = cast_i64(&lhs_value);
            let rhs = cast_i64(&rhs_value);
            TUdf::eval_numeric_i64(&mut collector, lhs, rhs)
        }
        UdfTarget::NumericF32 => {
            let lhs = cast_f32(&lhs_value);
            let rhs = cast_f32(&rhs_value);
            TUdf::eval_numeric_f32(&mut collector, lhs, rhs)
        }
        UdfTarget::NumericF64 => {
            let lhs = cast_f64(&lhs_value);
            let rhs = cast_f64(&rhs_value);
            TUdf::eval_numeric_f64(&mut collector, lhs, rhs)
        }
        _ => not_impl_err!("dispatch_binary_scalar_scalar"),
    }?;

    collector.finish_columnar_value()
}

fn extract_scalar_value(value: ScalarValue) -> DFResult<(EncTermField, Box<ScalarValue>)> {
    if let ScalarValue::Union(Some((type_id, value)), _, _) = value {
        Ok((type_id.try_into()?, value))
    } else {
        exec_err!("Unexpected lhs scalar in binary operation")
    }
}

fn decide_type<TUdf>(lhs_field: EncTermField, rhs_field: EncTermField) -> UdfTarget
where
    TUdf: EncScalarBinaryUdf,
{
    if TUdf::supports_named_node() {
        if let Some(value) = try_find_named_node_type(lhs_field, rhs_field) {
            return value;
        }
    }

    if TUdf::supports_blank_node() {
        if let Some(value) = try_find_blank_node_type(lhs_field, rhs_field) {
            return value;
        }
    }

    if TUdf::supports_numeric() {
        if let Some(value) = try_find_numeric_type(lhs_field, rhs_field) {
            return value;
        }
    }

    if TUdf::supports_boolean() {
        if let Some(value) = try_find_boolean_type(lhs_field, rhs_field) {
            return value;
        }
    }

    if TUdf::supports_string() {
        if let Some(value) = try_find_string_type(lhs_field, rhs_field) {
            return value;
        }
    }

    if TUdf::supports_date_time() {
        if let Some(value) = try_find_date_time_type(lhs_field, rhs_field) {
            return value;
        }
    }

    if TUdf::supports_simple_literal() {
        if let Some(value) = try_find_simple_literal_type(lhs_field, rhs_field) {
            return value;
        }
    }

    if TUdf::supports_typed_literal() {
        if let Some(value) = try_find_typed_literal_type(lhs_field, rhs_field) {
            return value;
        }
    }

    UdfTarget::Incompatible
}

fn try_find_named_node_type(lhs_field: EncTermField, rhs_field: EncTermField) -> Option<UdfTarget> {
    match (lhs_field, rhs_field) {
        (EncTermField::NamedNode, EncTermField::NamedNode) => Some(UdfTarget::NamedNode),
        _ => None,
    }
}

fn try_find_blank_node_type(lhs_field: EncTermField, rhs_field: EncTermField) -> Option<UdfTarget> {
    match (lhs_field, rhs_field) {
        (EncTermField::BlankNode, EncTermField::BlankNode) => Some(UdfTarget::BlankNode),
        _ => None,
    }
}

fn try_find_numeric_type(lhs_field: EncTermField, rhs_field: EncTermField) -> Option<UdfTarget> {
    match (lhs_field, rhs_field) {
        (EncTermField::Int, EncTermField::Int) => Some(UdfTarget::NumericI32),
        (EncTermField::Int, EncTermField::Integer) => Some(UdfTarget::NumericI64),
        (EncTermField::Int, EncTermField::Float32) => Some(UdfTarget::NumericF32),
        (EncTermField::Int, EncTermField::Float64) => Some(UdfTarget::NumericF64),

        (EncTermField::Integer, EncTermField::Int) => Some(UdfTarget::NumericI64),
        (EncTermField::Integer, EncTermField::Integer) => Some(UdfTarget::NumericI64),
        (EncTermField::Integer, EncTermField::Float32) => Some(UdfTarget::NumericF64), // TODO @tobixdev: Check this
        (EncTermField::Integer, EncTermField::Float64) => Some(UdfTarget::NumericF64),

        (EncTermField::Float32, EncTermField::Int) => Some(UdfTarget::NumericF32),
        (EncTermField::Float32, EncTermField::Integer) => Some(UdfTarget::NumericF64), // TODO @tobixdev: Check this
        (EncTermField::Float32, EncTermField::Float32) => Some(UdfTarget::NumericF32),
        (EncTermField::Float32, EncTermField::Float64) => Some(UdfTarget::NumericF64),

        (EncTermField::Float64, EncTermField::Int) => Some(UdfTarget::NumericF64),
        (EncTermField::Float64, EncTermField::Integer) => Some(UdfTarget::NumericF64), // TODO @tobixdev: Check this
        (EncTermField::Float64, EncTermField::Float32) => Some(UdfTarget::NumericF64),
        (EncTermField::Float64, EncTermField::Float64) => Some(UdfTarget::NumericF64),
        _ => None,
    }
}

fn try_find_boolean_type(lhs_field: EncTermField, rhs_field: EncTermField) -> Option<UdfTarget> {
    match (lhs_field, rhs_field) {
        (EncTermField::Boolean, EncTermField::Boolean) => Some(UdfTarget::Boolean),
        _ => None,
    }
}

fn try_find_string_type(lhs_field: EncTermField, rhs_field: EncTermField) -> Option<UdfTarget> {
    match (lhs_field, rhs_field) {
        (EncTermField::String, EncTermField::String) => Some(UdfTarget::String),
        _ => None,
    }
}

fn try_find_date_time_type(_: EncTermField, _: EncTermField) -> Option<UdfTarget> {
    todo!()
}

fn try_find_simple_literal_type(_: EncTermField, _: EncTermField) -> Option<UdfTarget> {
    todo!()
}

fn try_find_typed_literal_type(_: EncTermField, _: EncTermField) -> Option<UdfTarget> {
    todo!()
}

#[derive(Debug)]
enum UdfTarget {
    NamedNode,
    BlankNode,
    NumericI32,
    NumericI64,
    NumericF32,
    NumericF64,
    Boolean,
    String,
    DateTime,
    SimpleLiteral,
    TypedLiteral,
    Incompatible,
}
