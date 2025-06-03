use crate::builtin::BuiltinName;
use crate::scalar::dynamic_udf::DynamicRdfFusionUdf;
use crate::scalar::plain_term::str_plain_term;
use crate::scalar::typed_value::{
    bnode_nullary_typed_value, bnode_unary_typed_value, regex_binary_typed_value,
    regex_ternary_typed_value, replace_flags_typed_value, replace_typed_value, str_typed_value,
    sub_str_binary_typed_value, sub_str_ternary_typed_value,
};
use crate::FunctionName;
use datafusion::logical_expr::ScalarUDF;
use std::sync::Arc;

#[macro_use]
mod binary;
#[macro_use]
mod nullary;
#[macro_use]
mod quaternary;
#[macro_use]
mod ternary;
pub(crate) mod plain_term;
pub(crate) mod typed_value;
#[macro_use]
mod unary;
#[macro_use]
mod n_ary;
mod dynamic_udf;

pub fn str() -> Arc<ScalarUDF> {
    let udf = DynamicRdfFusionUdf::try_new(
        FunctionName::Builtin(BuiltinName::Str),
        vec![
            str_plain_term().as_ref().clone(),
            str_typed_value().as_ref().clone(),
        ],
    )
    .expect("UDFs are compatible");
    Arc::new(ScalarUDF::new_from_impl(udf))
}

pub fn bnode() -> Arc<ScalarUDF> {
    let udf = DynamicRdfFusionUdf::try_new(
        FunctionName::Builtin(BuiltinName::BNode),
        vec![
            bnode_nullary_typed_value().as_ref().clone(),
            bnode_unary_typed_value().as_ref().clone(),
        ],
    )
    .expect("UDFs are compatible");
    Arc::new(ScalarUDF::new_from_impl(udf))
}

pub fn sub_str() -> Arc<ScalarUDF> {
    let udf = DynamicRdfFusionUdf::try_new(
        FunctionName::Builtin(BuiltinName::SubStr),
        vec![
            sub_str_binary_typed_value().as_ref().clone(),
            sub_str_ternary_typed_value().as_ref().clone(),
        ],
    )
    .expect("UDFs are compatible");
    Arc::new(ScalarUDF::new_from_impl(udf))
}

pub fn regex() -> Arc<ScalarUDF> {
    let udf = DynamicRdfFusionUdf::try_new(
        FunctionName::Builtin(BuiltinName::Regex),
        vec![
            regex_binary_typed_value().as_ref().clone(),
            regex_ternary_typed_value().as_ref().clone(),
        ],
    )
    .expect("UDFs are compatible");
    Arc::new(ScalarUDF::new_from_impl(udf))
}

pub fn replace() -> Arc<ScalarUDF> {
    let udf = DynamicRdfFusionUdf::try_new(
        FunctionName::Builtin(BuiltinName::Replace),
        vec![
            replace_typed_value().as_ref().clone(),
            replace_flags_typed_value().as_ref().clone(),
        ],
    )
    .expect("UDFs are compatible");
    Arc::new(ScalarUDF::new_from_impl(udf))
}
