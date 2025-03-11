use crate::encoded::numeric::add::EncAdd;
use crate::encoded::numeric::div::EncDiv;
use crate::encoded::numeric::mul::EncMul;
use crate::encoded::numeric::sub::EncSub;
use crate::encoded::numeric::unary_minus::EncUnaryMinus;
use crate::encoded::numeric::unary_plus::EncUnaryPlus;
use datafusion::logical_expr::ScalarUDF;
use once_cell::sync::Lazy;

mod add;
mod div;
mod mul;
mod sub;
mod unary_minus;
mod unary_plus;

pub const ENC_ADD: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncAdd::new()));
pub const ENC_DIV: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncDiv::new()));
pub const ENC_MUL: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncMul::new()));
pub const ENC_SUB: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncSub::new()));
pub const ENC_UNARY_MINUS: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncUnaryMinus::new()));
pub const ENC_UNARY_PLUS: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncUnaryPlus::new()));
