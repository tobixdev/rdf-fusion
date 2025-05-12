use crate::aggregates::{avg_typed_value, max_typed_value, min_typed_value, sum_typed_value};
use crate::builtin::encoding::{
    with_plain_term_encoding, with_sortable_term_encoding, with_typed_value_encoding,
};
use crate::builtin::logical::{sparql_and, sparql_or};
use crate::builtin::native::{effective_boolean_value, native_boolean_as_term};
use crate::builtin::query::is_compatible;
use crate::builtin::BuiltinName;
use crate::scalar::typed_value::{
    abs_typed_value, add_typed_value, as_boolean_typed_value, as_date_time_typed_value,
    as_decimal_typed_value, as_double_typed_value, as_float_typed_value, as_int_typed_value,
    as_integer_typed_value, as_string_typed_value, bnode_typed_value, bound_typed_value,
    ceil_typed_value, coalesce_typed_value, concat_typed_value, contains_typed_value,
    datatype_typed_value, day_typed_value, div_typed_value, encode_for_uri_typed_value,
    eq_typed_value, floor_typed_value, greater_or_equal_typed_value, greater_than_typed_value,
    hours_typed_value, if_typed_value, is_blank_typed_value, is_iri_typed_value,
    is_literal_typed_value, is_numeric_typed_value, lang_matches_typed_value, lang_typed_value,
    lcase_typed_value, less_or_equal_typed_value, less_than_typed_value, md5_typed_value,
    minutes_typed_value, month_typed_value, mul_typed_value, rand_typed_value,
    regex_binary_typed_value, replace_typed_value, round_typed_value, same_term_typed_value,
    seconds_typed_value, sha1_typed_value, sha256_typed_value, sha384_typed_value,
    sha512_typed_value, str_after_typed_value, str_before_typed_value, str_dt_typed_value,
    str_ends_typed_value, str_lang_typed_value, str_len_typed_value, str_starts_typed_value,
    str_typed_value, str_uuid_typed_value, sub_str_ternary_typed_value, sub_typed_value,
    timezone_typed_value, tz_typed_value, ucase_typed_value, unary_minus_typed_value,
    unary_plus_typed_value, uuid_typed_value, year_typed_value,
};
use crate::{DFResult, FunctionName};
use datafusion::common::plan_err;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use graphfusion_model::Term;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

/// TODO
pub type GraphFusionFunctionRegistryRef = Arc<dyn GraphFusionFunctionRegistry>;

///TODO
pub trait GraphFusionFunctionRegistry: Debug + Send + Sync {
    /// Creates a DataFusion [ScalarUDF] given the `constant_args`.
    fn create_udf(
        &self,
        function_name: FunctionName,
        constant_args: HashMap<String, Term>,
    ) -> DFResult<Arc<ScalarUDF>>;

    /// Creates a DataFusion [AggregateUDF] given the `constant_args`.
    fn create_udaf(
        &self,
        function_name: FunctionName,
        constant_args: HashMap<String, Term>,
    ) -> DFResult<Arc<AggregateUDF>>;
}

/// TODO
#[derive(Debug, Default)]
pub struct DefaultGraphFusionFunctionRegistry {}

impl GraphFusionFunctionRegistry for DefaultGraphFusionFunctionRegistry {
    fn create_udf(
        &self,
        function_name: FunctionName,
        constant_args: HashMap<String, Term>,
    ) -> DFResult<Arc<ScalarUDF>> {
        match function_name {
            FunctionName::Builtin(builtin_name) => Ok(match builtin_name {
                BuiltinName::Str => str_typed_value(),
                BuiltinName::Lang => lang_typed_value(),
                BuiltinName::LangMatches => lang_matches_typed_value(),
                BuiltinName::Datatype => datatype_typed_value(),
                BuiltinName::Iri => todo!("Group concat is not supported yet (extract base_iri)."),
                BuiltinName::BNode => bnode_typed_value(),
                BuiltinName::Rand => rand_typed_value(),
                BuiltinName::Abs => abs_typed_value(),
                BuiltinName::Ceil => ceil_typed_value(),
                BuiltinName::Floor => floor_typed_value(),
                BuiltinName::Round => round_typed_value(),
                BuiltinName::Concat => concat_typed_value(),
                BuiltinName::SubStr => sub_str_ternary_typed_value(),
                BuiltinName::StrLen => str_len_typed_value(),
                BuiltinName::Replace => replace_typed_value(),
                BuiltinName::UCase => ucase_typed_value(),
                BuiltinName::LCase => lcase_typed_value(),
                BuiltinName::EncodeForUri => encode_for_uri_typed_value(),
                BuiltinName::Contains => contains_typed_value(),
                BuiltinName::StrStarts => str_starts_typed_value(),
                BuiltinName::StrEnds => str_ends_typed_value(),
                BuiltinName::StrBefore => str_before_typed_value(),
                BuiltinName::StrAfter => str_after_typed_value(),
                BuiltinName::Year => year_typed_value(),
                BuiltinName::Month => month_typed_value(),
                BuiltinName::Day => day_typed_value(),
                BuiltinName::Hours => hours_typed_value(),
                BuiltinName::Minutes => minutes_typed_value(),
                BuiltinName::Seconds => seconds_typed_value(),
                BuiltinName::Timezone => timezone_typed_value(),
                BuiltinName::Tz => tz_typed_value(),
                BuiltinName::Uuid => uuid_typed_value(),
                BuiltinName::StrUuid => str_uuid_typed_value(),
                BuiltinName::Md5 => md5_typed_value(),
                BuiltinName::Sha1 => sha1_typed_value(),
                BuiltinName::Sha256 => sha256_typed_value(),
                BuiltinName::Sha384 => sha384_typed_value(),
                BuiltinName::Sha512 => sha512_typed_value(),
                BuiltinName::StrLang => str_lang_typed_value(),
                BuiltinName::StrDt => str_dt_typed_value(),
                BuiltinName::IsIri => is_iri_typed_value(),
                BuiltinName::IsBlank => is_blank_typed_value(),
                BuiltinName::IsLiteral => is_literal_typed_value(),
                BuiltinName::IsNumeric => is_numeric_typed_value(),
                BuiltinName::Regex => regex_binary_typed_value(),
                BuiltinName::Bound => bound_typed_value(),
                BuiltinName::Coalesce => coalesce_typed_value(),
                BuiltinName::If => if_typed_value(),
                BuiltinName::SameTerm => same_term_typed_value(),
                BuiltinName::Equal => eq_typed_value(),
                BuiltinName::GreaterThan => greater_than_typed_value(),
                BuiltinName::GreaterOrEqual => greater_or_equal_typed_value(),
                BuiltinName::LessThan => less_than_typed_value(),
                BuiltinName::LessOrEqual => less_or_equal_typed_value(),
                BuiltinName::Add => add_typed_value(),
                BuiltinName::Div => div_typed_value(),
                BuiltinName::Mul => mul_typed_value(),
                BuiltinName::Sub => sub_typed_value(),
                BuiltinName::UnaryMinus => unary_minus_typed_value(),
                BuiltinName::UnaryPlus => unary_plus_typed_value(),
                BuiltinName::And => sparql_and(),
                BuiltinName::Or => sparql_or(),
                BuiltinName::AsString => as_string_typed_value(),
                BuiltinName::AsInteger => as_integer_typed_value(),
                BuiltinName::AsInt => as_int_typed_value(),
                BuiltinName::AsFloat => as_float_typed_value(),
                BuiltinName::AsDouble => as_double_typed_value(),
                BuiltinName::AsDecimal => as_decimal_typed_value(),
                BuiltinName::AsDateTime => as_date_time_typed_value(),
                BuiltinName::AsBoolean => as_boolean_typed_value(),
                BuiltinName::WithSortableEncoding => with_sortable_term_encoding(),
                BuiltinName::WithTypedValueEncoding => with_typed_value_encoding(),
                BuiltinName::WithPlainTermEncoding => with_plain_term_encoding(),
                BuiltinName::EffectiveBooleanValue => effective_boolean_value(),
                BuiltinName::NativeBooleanAsTerm => native_boolean_as_term(),
                BuiltinName::IsCompatible => is_compatible(),
                _ => return plan_err!("Unsupported scalar function"),
            }),
            FunctionName::Custom(_) => plan_err!("Custom functions are not supported yet."),
        }
    }

    fn create_udaf(
        &self,
        function_name: FunctionName,
        constant_args: HashMap<String, Term>,
    ) -> DFResult<Arc<AggregateUDF>> {
        match function_name {
            FunctionName::Builtin(builtin) => Ok(match builtin {
                BuiltinName::Sum => sum_typed_value(),
                BuiltinName::Min => min_typed_value(),
                BuiltinName::Max => max_typed_value(),
                BuiltinName::Avg => avg_typed_value(),
                BuiltinName::GroupConcat => {
                    todo!("Group concat is not supported yet (extract seaprator).")
                }
                _ => return plan_err!("Unsupported aggregate function"),
            }),
            FunctionName::Custom(_) => plan_err!("Custom functions are not supported yet."),
        }
    }
}
