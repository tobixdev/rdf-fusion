#[macro_use]
mod quaternary;
#[macro_use]
mod ternary;
#[macro_use]
mod n_ary;
mod args;
pub mod comparison;
pub mod conversion;
pub mod dates_and_times;
mod dispatch;
mod dynamic_udf;
pub mod functional_form;
pub mod numeric;
mod sparql_op;
pub mod strings;
pub mod terms;
pub mod typed_value;

use crate::builtin::BuiltinName;
use crate::scalar::dynamic_udf::DynamicRdfFusionUdf;
use crate::scalar::typed_value::{replace_flags_typed_value, replace_typed_value};
use crate::FunctionName;
pub use args::*;
use datafusion::logical_expr::ScalarUDF;
pub use sparql_op::*;
use std::sync::Arc;

#[allow(clippy::expect_used, reason = "UDFs are known at compile time")]
pub fn replace() -> Arc<ScalarUDF> {
    let udf = DynamicRdfFusionUdf::try_new(
        &FunctionName::Builtin(BuiltinName::Replace),
        &[
            replace_typed_value().as_ref().clone(),
            replace_flags_typed_value().as_ref().clone(),
        ],
    )
    .expect("UDFs are compatible");
    Arc::new(ScalarUDF::new_from_impl(udf))
}
