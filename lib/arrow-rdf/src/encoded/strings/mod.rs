use crate::encoded::strings::lcase::EncLCase;
use crate::encoded::strings::strlen::EncStrLen;
use crate::encoded::strings::substr::EncSubStr;
use crate::encoded::strings::ucase::EncUCase;
use datafusion::logical_expr::ScalarUDF;
use once_cell::unsync::Lazy;

mod lcase;
mod strlen;
mod substr;
mod ucase;

pub const ENC_STRLEN: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncStrLen::new()));
pub const ENC_SUBSTR: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncSubStr::new()));
pub const ENC_UCASE: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncUCase::new()));
pub const ENC_LCASE: Lazy<ScalarUDF> = Lazy::new(|| ScalarUDF::from(EncLCase::new()));
