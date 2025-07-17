use rdf_fusion_model::ThinError;
use rdf_fusion_model::{SimpleLiteral, TypedValue, TypedValueRef};

use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_unary_owned_typed_value;
use crate::scalar::{ScalarSparqlOp, UnaryArgs};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};

#[derive(Debug)]
pub struct CastStringSparqlOp;

impl Default for CastStringSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CastStringSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::CastBoolean);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for CastStringSparqlOp {
    type Args<TEncoding: TermEncoding> = UnaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn supported_encodings(&self) -> &[EncodingName] {
        &[EncodingName::TypedValue]
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn return_type(&self, input_encoding: Option<EncodingName>) -> DFResult<DataType> {
        if !matches!(input_encoding, Some(EncodingName::TypedValue)) {
            return exec_err!("Unexpected target encoding: {:?}", input_encoding);
        }
        Ok(TypedValueEncoding::data_type())
    }

    fn invoke_typed_value_encoding(
        &self,
        UnaryArgs(arg): Self::Args<TypedValueEncoding>,
    ) -> DFResult<ColumnarValue> {
        dispatch_unary_owned_typed_value(
            &arg,
            |value| {
                let converted = match value {
                    TypedValueRef::NamedNode(value) => value.as_str().to_owned(),
                    TypedValueRef::BlankNode(_) => return ThinError::expected(),
                    TypedValueRef::BooleanLiteral(value) => value.to_string(),
                    TypedValueRef::NumericLiteral(value) => value.format_value(),
                    TypedValueRef::SimpleLiteral(value) => value.value.to_owned(),
                    TypedValueRef::LanguageStringLiteral(value) => value.value.to_owned(),
                    TypedValueRef::DateTimeLiteral(value) => value.to_string(),
                    TypedValueRef::TimeLiteral(value) => value.to_string(),
                    TypedValueRef::DateLiteral(value) => value.to_string(),
                    TypedValueRef::DurationLiteral(value) => value.to_string(),
                    TypedValueRef::YearMonthDurationLiteral(value) => value.to_string(),
                    TypedValueRef::DayTimeDurationLiteral(value) => value.to_string(),
                    TypedValueRef::OtherLiteral(value) => value.value().to_owned(),
                };
                Ok(TypedValue::SimpleLiteral(SimpleLiteral {
                    value: converted,
                }))
            },
            || ThinError::expected(),
        )
    }
}
