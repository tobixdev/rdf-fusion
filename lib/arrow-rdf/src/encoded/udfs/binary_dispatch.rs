use crate::encoded::udfs::result_collector::ResultCollector;
use crate::{as_rdf_term_array, encoded, DFResult};
use datafusion::arrow::array::{Array, ArrayAccessor, AsArray, UnionArray};
use datafusion::arrow::datatypes::{Float32Type, Float64Type, Int32Type, Int64Type};
use datafusion::common::{
    exec_err, not_impl_datafusion_err, not_impl_err, DataFusionError, ScalarValue,
};
use datafusion::logical_expr::ColumnarValue;
use oxrdf::vocab::xsd;
use std::sync::Arc;

pub enum UdfResult {
    TermBoolean(bool),
    TermInt(i32),
    TermInteger(i64),
    TermFloat32(f32),
    TermFloat64(f64),
    TermString(String),
}

impl UdfResult {
    #[inline(always)]
    pub fn into_i32(self) -> i32 {
        match self {
            UdfResult::TermInt(value) => value,
            _ => panic!("expected i32"),
        }
    }

    #[inline(always)]
    pub fn into_i64(self) -> i64 {
        match self {
            UdfResult::TermInteger(value) => value,
            _ => panic!("expected i32"),
        }
    }

    #[inline(always)]
    pub fn into_f32(self) -> f32 {
        match self {
            UdfResult::TermFloat32(value) => value,
            _ => panic!("expected i32"),
        }
    }

    #[inline(always)]
    pub fn into_f64(self) -> f64 {
        match self {
            UdfResult::TermFloat64(value) => value,
            _ => panic!("expected i32"),
        }
    }
}

pub trait EncScalarBinaryUdf {
    type Collector: ResultCollector;

    fn supports_named_node() -> bool;
    fn supports_blank_node() -> bool;
    fn supports_numeric() -> bool;
    fn supports_boolean() -> bool;
    fn supports_string() -> bool;
    fn supports_date_time() -> bool;
    fn supports_simple_literal() -> bool;

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
    lhs: Arc<dyn Array>,
    rhs: Arc<dyn Array>,
) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarBinaryUdf,
{
    Err(not_impl_datafusion_err!("dispatch_binary_array_array"))
}

fn dispatch_binary_scalar_array<TUdf>(
    lhs: ScalarValue,
    rhs: Arc<dyn Array>,
) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarBinaryUdf,
{
    let ScalarValue::Union(element, _, _) = lhs else {
        return exec_err!("Unexpected type for scalar.");
    };

    match element {
        None => not_impl_err!("dispatch_binary_scalar_array: None Case"),
        Some((idx, element)) => not_impl_err!("dispatch_binary_scalar_array: Some Case"),
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
        .zip(rhs.offsets().expect("Always Dense"));

    let mut collector = TUdf::Collector::new();
    for (rhs_type, rhs_offset) in type_offset_paris {
        match decide_type::<TUdf>(lhs_type, *rhs_type) {
            UdfTarget::NamedNode => {
                let lhs = cast_str(&lhs_value);
                let rhs = cast_str_arr(rhs, *rhs_type, *rhs_offset as usize);
                TUdf::eval_named_node(&mut collector, lhs, rhs)?;
            }
            UdfTarget::BlankNode => {
                let lhs = cast_str(&lhs_value);
                let rhs = cast_str_arr(rhs, *rhs_type, *rhs_offset as usize);
                TUdf::eval_blank_node(&mut collector, lhs, rhs)?;
            }
            UdfTarget::NumericI32 => {
                let lhs = cast_i32(&lhs_value);
                let rhs = cast_i32_arr(rhs, *rhs_type, *rhs_offset as usize);
                TUdf::eval_numeric_i32(&mut collector, lhs, rhs)?;
            }
            UdfTarget::NumericI64 => {
                let lhs = cast_i64(&lhs_value);
                let rhs = cast_i64_arr(rhs, *rhs_type, *rhs_offset as usize);
                TUdf::eval_numeric_i64(&mut collector, lhs, rhs)?;
            }
            UdfTarget::NumericF32 => {
                let lhs = cast_f32(&lhs_value);
                let rhs = cast_f32_arr(rhs, *rhs_type, *rhs_offset as usize);
                TUdf::eval_numeric_f32(&mut collector, lhs, rhs)?;
            }
            UdfTarget::NumericF64 => {
                let lhs = cast_f64(&lhs_value);
                let rhs = cast_f64_arr(rhs, *rhs_type, *rhs_offset as usize);
                TUdf::eval_numeric_f64(&mut collector, lhs, rhs)?;
            }
            UdfTarget::TypedLiteral => {
                let (lhs_value, lhs_type) = cast_typed_literal(lhs_type, &lhs_value)?;
                let (rhs_value, rhs_type) =
                    cast_typed_literal_array(rhs, *rhs_type, *rhs_offset as usize);
                TUdf::eval_typed_literal(
                    &mut collector,
                    &lhs_value,
                    &lhs_type,
                    &rhs_value,
                    &rhs_type,
                )?;
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

fn cast_typed_literal(type_id: i8, scalar: &ScalarValue) -> DFResult<(String, &str)> {
    let value = cast_string(scalar);
    let datatype = match type_id {
        encoded::ENC_TYPE_ID_INTEGER => xsd::INTEGER.as_str(),
        encoded::ENC_TYPE_ID_TYPED_LITERAL => {
            if let ScalarValue::Struct(scalar) = scalar {
                let datatype = scalar
                    .column_by_name("datatype")
                    .unwrap()
                    .as_string::<i32>()
                    .value(0);
                datatype
            } else {
                return exec_err!("Unexpected scalar for typed literal")
            }
        }
        _ => return not_impl_err!("cast_typed_literal datatype for {type_id}")
    };

    Ok((value, datatype))
}

fn cast_string(value: &ScalarValue) -> String {
    // TODO @tobixdev with type id?
    value.to_string()
}

fn cast_typed_literal_array(rdf_terms: &UnionArray, type_id: i8, offset: usize) -> (String, &str) {
    match type_id {
        encoded::ENC_TYPE_ID_INT => {
            let value = rdf_terms
                .child(type_id)
                .as_primitive::<Int32Type>()
                .value(offset);
            (value.to_string(), xsd::INT.as_str())
        }
        encoded::ENC_TYPE_ID_INTEGER => {
            let value = rdf_terms
                .child(type_id)
                .as_primitive::<Int64Type>()
                .value(offset);
            (value.to_string(), xsd::INTEGER.as_str())
        }
        encoded::ENC_TYPE_ID_FLOAT32 => {
            let value = rdf_terms
                .child(type_id)
                .as_primitive::<Float32Type>()
                .value(offset);
            (value.to_string(), xsd::FLOAT.as_str())
        }
        encoded::ENC_TYPE_ID_FLOAT64 => {
            let value = rdf_terms
                .child(type_id)
                .as_primitive::<Float64Type>()
                .value(offset);
            (value.to_string(), xsd::DOUBLE.as_str())
        }
        encoded::ENC_TYPE_ID_BOOLEAN => {
            let value = rdf_terms.child(type_id).as_boolean().value(offset);
            (value.to_string(), xsd::BOOLEAN.as_str())
        }
        encoded::ENC_TYPE_ID_STRING => {
            let value = rdf_terms.child(type_id).as_string::<i32>().value(offset);
            (value.to_string(), xsd::STRING.as_str())
        }
        encoded::ENC_TYPE_ID_TYPED_LITERAL => {
            let inner = rdf_terms.child(type_id).as_struct();
            let value = inner
                .column_by_name("value")
                .unwrap()
                .as_string::<i32>()
                .value(offset);
            let datatype = inner
                .column_by_name("datatype")
                .unwrap()
                .as_string::<i32>()
                .value(offset);
            (value.to_string(), datatype)
        }
        _ => panic!("Expected castable to str"),
    }
}

fn cast_str(scalar: &ScalarValue) -> &str {
    match scalar {
        ScalarValue::Utf8(value) => value.as_ref().unwrap(),
        ScalarValue::Utf8View(value) => value.as_ref().unwrap(),
        ScalarValue::LargeUtf8(value) => value.as_ref().unwrap(),
        _ => panic!("epxected castable to i32"),
    }
}

fn cast_str_arr(rdf_terms: &UnionArray, type_id: i8, offset: usize) -> &str {
    match type_id {
        encoded::ENC_TYPE_ID_NAMED_NODE => {
            rdf_terms.child(type_id).as_string::<i32>().value(offset)
        }
        encoded::ENC_TYPE_ID_BLANK_NODE => {
            rdf_terms.child(type_id).as_string::<i32>().value(offset)
        }
        _ => panic!("Expected castable to str"),
    }
}

fn cast_i32(scalar: &ScalarValue) -> i32 {
    match scalar {
        ScalarValue::Int8(value) => value.unwrap() as i32,
        ScalarValue::Int16(value) => value.unwrap() as i32,
        ScalarValue::Int32(value) => value.unwrap(),
        ScalarValue::UInt8(value) => value.unwrap() as i32,
        ScalarValue::UInt16(value) => value.unwrap() as i32,
        _ => panic!("epxected castable to i32"),
    }
}

fn cast_i32_arr(rdf_terms: &UnionArray, type_id: i8, offset: usize) -> i32 {
    match type_id {
        encoded::ENC_TYPE_ID_INT => rdf_terms
            .child(type_id)
            .as_primitive::<Int32Type>()
            .value(offset),
        _ => panic!("Expected castable to i32"),
    }
}

fn cast_i64(scalar: &ScalarValue) -> i64 {
    match *scalar {
        ScalarValue::Int8(value) => value.unwrap() as i64,
        ScalarValue::Int16(value) => value.unwrap() as i64,
        ScalarValue::Int32(value) => value.unwrap() as i64,
        ScalarValue::Int64(value) => value.unwrap(),
        ScalarValue::UInt8(value) => value.unwrap() as i64,
        ScalarValue::UInt16(value) => value.unwrap() as i64,
        ScalarValue::UInt32(value) => value.unwrap() as i64,
        _ => panic!("epxected castable to i64"),
    }
}

fn cast_i64_arr(rdf_terms: &UnionArray, type_id: i8, offset: usize) -> i64 {
    match type_id {
        encoded::ENC_TYPE_ID_INT => rdf_terms
            .child(type_id)
            .as_primitive::<Int32Type>()
            .value(offset) as i64,
        encoded::ENC_TYPE_ID_INTEGER => rdf_terms
            .child(type_id)
            .as_primitive::<Int64Type>()
            .value(offset),
        _ => panic!("Expected castable to i64"),
    }
}

fn cast_f32(scalar: &ScalarValue) -> f32 {
    match *scalar {
        ScalarValue::Int8(value) => value.unwrap() as f32,
        ScalarValue::Int16(value) => value.unwrap() as f32,
        ScalarValue::Int32(value) => value.unwrap() as f32,
        ScalarValue::UInt8(value) => value.unwrap() as f32,
        ScalarValue::UInt16(value) => value.unwrap() as f32,
        ScalarValue::UInt32(value) => value.unwrap() as f32,
        ScalarValue::Float32(value) => value.unwrap(),
        _ => panic!("epxected castable to f32"),
    }
}

fn cast_f32_arr(rdf_terms: &UnionArray, type_id: i8, offset: usize) -> f32 {
    match type_id {
        encoded::ENC_TYPE_ID_INT => rdf_terms
            .child(type_id)
            .as_primitive::<Int32Type>()
            .value(offset) as f32,
        encoded::ENC_TYPE_ID_FLOAT32 => rdf_terms
            .child(type_id)
            .as_primitive::<Float32Type>()
            .value(offset),
        _ => panic!("Expected castable to f32"),
    }
}

fn cast_f64(scalar: &ScalarValue) -> f64 {
    match *scalar {
        ScalarValue::Int8(value) => value.unwrap() as f64,
        ScalarValue::Int16(value) => value.unwrap() as f64,
        ScalarValue::Int32(value) => value.unwrap() as f64,
        ScalarValue::Int64(value) => value.unwrap() as f64,
        ScalarValue::UInt8(value) => value.unwrap() as f64,
        ScalarValue::UInt16(value) => value.unwrap() as f64,
        ScalarValue::UInt32(value) => value.unwrap() as f64,
        ScalarValue::UInt64(value) => value.unwrap() as f64,
        ScalarValue::Float32(value) => value.unwrap() as f64,
        ScalarValue::Float64(value) => value.unwrap(),
        _ => panic!("epxected castable to f64"),
    }
}

fn cast_f64_arr(rdf_terms: &UnionArray, type_id: i8, offset: usize) -> f64 {
    match type_id {
        encoded::ENC_TYPE_ID_INT => rdf_terms
            .child(type_id)
            .as_primitive::<Int32Type>()
            .value(offset) as f64,
        encoded::ENC_TYPE_ID_INTEGER => rdf_terms
            .child(type_id)
            .as_primitive::<Int64Type>()
            .value(offset) as f64,
        encoded::ENC_TYPE_ID_FLOAT32 => rdf_terms
            .child(type_id)
            .as_primitive::<Float32Type>()
            .value(offset) as f64,
        encoded::ENC_TYPE_ID_FLOAT64 => rdf_terms
            .child(type_id)
            .as_primitive::<Float64Type>()
            .value(offset),
        _ => panic!("Expected castable to f64"),
    }
}

#[inline(always)]
fn extract_scalar_value(value: ScalarValue) -> DFResult<(i8, Box<ScalarValue>)> {
    if let ScalarValue::Union(Some(value), _, _) = value {
        Ok(value)
    } else {
        exec_err!("Unexpected lhs scalar in binary operation")
    }
}

#[inline(always)]
fn decide_type<TUdf>(lhs_type_id: i8, rhs_type_id: i8) -> UdfTarget
where
    TUdf: EncScalarBinaryUdf,
{
    if TUdf::supports_named_node() {
        if let Some(value) = try_find_named_node_type(lhs_type_id, rhs_type_id) {
            return value;
        }
    }

    if TUdf::supports_numeric() {
        if let Some(value) = try_find_blank_node_type(lhs_type_id, rhs_type_id) {
            return value;
        }
    }

    if TUdf::supports_boolean() {
        if let Some(value) = try_find_boolean_type(lhs_type_id, rhs_type_id) {
            return value;
        }
    }

    if TUdf::supports_string() {
        if let Some(value) = try_find_string_type(lhs_type_id, rhs_type_id) {
            return value;
        }
    }

    if TUdf::supports_date_time() {
        if let Some(value) = try_find_date_time_type(lhs_type_id, rhs_type_id) {
            return value;
        }
    }

    if TUdf::supports_simple_literal() {
        if let Some(value) = try_find_simple_literal_type(lhs_type_id, rhs_type_id) {
            return value;
        }
    }

    UdfTarget::TypedLiteral
}

fn try_find_named_node_type(lhs_type_id: i8, rhs_type_id: i8) -> Option<UdfTarget> {
    match (lhs_type_id, rhs_type_id) {
        (encoded::ENC_TYPE_ID_NAMED_NODE, encoded::ENC_TYPE_ID_NAMED_NODE) => {
            Some(UdfTarget::NamedNode)
        }
        _ => None,
    }
}

fn try_find_blank_node_type(lhs_type_id: i8, rhs_type_id: i8) -> Option<UdfTarget> {
    match (lhs_type_id, rhs_type_id) {
        (encoded::ENC_TYPE_ID_BLANK_NODE, encoded::ENC_TYPE_ID_BLANK_NODE) => {
            Some(UdfTarget::BlankNode)
        }
        _ => None,
    }
}

fn try_find_numeric_type(lhs_type_id: i8, rhs_type_id: i8) -> Option<UdfTarget> {
    match (lhs_type_id, rhs_type_id) {
        (encoded::ENC_TYPE_ID_INT, encoded::ENC_TYPE_ID_INT) => Some(UdfTarget::NumericI32),
        (encoded::ENC_TYPE_ID_INT, encoded::ENC_TYPE_ID_INTEGER) => Some(UdfTarget::NumericI64),
        (encoded::ENC_TYPE_ID_INT, encoded::ENC_TYPE_ID_FLOAT32) => Some(UdfTarget::NumericF32),
        (encoded::ENC_TYPE_ID_INT, encoded::ENC_TYPE_ID_FLOAT64) => Some(UdfTarget::NumericF64),

        (encoded::ENC_TYPE_ID_INTEGER, encoded::ENC_TYPE_ID_INT) => Some(UdfTarget::NumericI64),
        (encoded::ENC_TYPE_ID_INTEGER, encoded::ENC_TYPE_ID_INTEGER) => Some(UdfTarget::NumericI64),
        (encoded::ENC_TYPE_ID_INTEGER, encoded::ENC_TYPE_ID_FLOAT32) => Some(UdfTarget::NumericF64), // TODO @tobixdev: Check this
        (encoded::ENC_TYPE_ID_INTEGER, encoded::ENC_TYPE_ID_FLOAT64) => Some(UdfTarget::NumericF64),

        (encoded::ENC_TYPE_ID_FLOAT32, encoded::ENC_TYPE_ID_INT) => Some(UdfTarget::NumericF32),
        (encoded::ENC_TYPE_ID_FLOAT32, encoded::ENC_TYPE_ID_INTEGER) => Some(UdfTarget::NumericF64), // TODO @tobixdev: Check this
        (encoded::ENC_TYPE_ID_FLOAT32, encoded::ENC_TYPE_ID_FLOAT32) => Some(UdfTarget::NumericF32),
        (encoded::ENC_TYPE_ID_FLOAT32, encoded::ENC_TYPE_ID_FLOAT64) => Some(UdfTarget::NumericF64),

        (encoded::ENC_TYPE_ID_FLOAT64, encoded::ENC_TYPE_ID_INT) => Some(UdfTarget::NumericF64),
        (encoded::ENC_TYPE_ID_FLOAT64, encoded::ENC_TYPE_ID_INTEGER) => Some(UdfTarget::NumericF64), // TODO @tobixdev: Check this
        (encoded::ENC_TYPE_ID_FLOAT64, encoded::ENC_TYPE_ID_FLOAT32) => Some(UdfTarget::NumericF64),
        (encoded::ENC_TYPE_ID_FLOAT64, encoded::ENC_TYPE_ID_FLOAT64) => Some(UdfTarget::NumericF64),
        _ => None,
    }
}

fn try_find_boolean_type(lhs_type_id: i8, rhs_type_id: i8) -> Option<UdfTarget> {
    match (lhs_type_id, rhs_type_id) {
        (encoded::ENC_TYPE_ID_BOOLEAN, encoded::ENC_TYPE_ID_BOOLEAN) => Some(UdfTarget::Boolean),
        _ => None,
    }
}

fn try_find_string_type(lhs_type_id: i8, rhs_type_id: i8) -> Option<UdfTarget> {
    match (lhs_type_id, rhs_type_id) {
        (encoded::ENC_TYPE_ID_STRING, encoded::ENC_TYPE_ID_STRING) => Some(UdfTarget::String),
        _ => None,
    }
}

fn try_find_date_time_type(_: i8, _: i8) -> Option<UdfTarget> {
    None
}

fn try_find_simple_literal_type(_: i8, _: i8) -> Option<UdfTarget> {
    None
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
}
