use crate::builtin::{BuiltinName, GraphFusionUdfFactory};
use crate::{DFResult, FunctionName};
use datafusion::arrow::array::{as_union_array, StructArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use graphfusion_encoding::plain_term::PlainTermEncoding;
use graphfusion_encoding::typed_value::TypedValueEncoding;
use graphfusion_encoding::{EncodingName, TermEncoding};
use graphfusion_model::{Term, ThinResult, TypedValueRef};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use graphfusion_encoding::sortable_term::SortableTermArrayBuilder;

#[derive(Debug)]
struct WithSortableEncodingFactory;

impl GraphFusionUdfFactory for WithSortableEncodingFactory {
    fn name(&self) -> FunctionName {
        FunctionName::Builtin(BuiltinName::WithSortableEncoding)
    }

    fn encoding(&self) -> Vec<EncodingName> {
        vec![EncodingName::TypedValue, EncodingName::PlainTerm]
    }

    fn create_with_args(&self, _constant_args: HashMap<String, Term>) -> DFResult<Arc<ScalarUDF>> {
        let udf = ScalarUDF::new_from_impl(
            WithSortableEncoding::new(self.name()),
        );
        Ok(Arc::new(udf))
    }
}

#[derive(Debug)]
pub struct WithSortableEncoding {
    name: String,
    signature: Signature,
}

impl WithSortableEncoding {
    pub fn new(name: FunctionName) -> Self {
        Self {
            name: name.to_string(),
            signature: Signature::new(
                TypeSignature::Uniform(
                    1,
                    vec![
                        PlainTermEncoding::data_type(),
                        TypedValueEncoding::data_type(),
                    ],
                ),
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for WithSortableEncoding {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(SortableTerm::data_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
        if args.args.len() != 1 {
            return exec_err!("Unexpected number of arguments");
        }

        match &args.args[0] {
            ColumnarValue::Array(array) => {
                let array = as_union_array(array);
                let values = (0..args.number_rows).map(|i| TypedValueRef::from_array(array, i));
                let result = into_struct_enc(values);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            ColumnarValue::Scalar(scalar) => {
                let term = TypedValueRef::from_scalar(scalar);
                let result = into_struct_enc([term]);
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &result, 0,
                )?))
            }
        }
    }
}

fn into_struct_enc<'data>(
    terms: impl IntoIterator<Item = ThinResult<TypedValueRef<'data>>>,
) -> StructArray {
    let terms_iter = terms.into_iter();

    let (_, size_upper_bound) = terms_iter.size_hint();
    let mut builder = SortableTermArrayBuilder::new(size_upper_bound.unwrap_or(0));

    for term in terms_iter {
        if let Ok(term) = term {
            match term {
                TypedValueRef::NamedNode(v) => builder.append_named_node(v),
                TypedValueRef::BlankNode(v) => builder.append_blank_node(v),
                TypedValueRef::BooleanLiteral(v) => builder.append_boolean(v),
                TypedValueRef::NumericLiteral(v) => {
                    builder.append_numeric(v, v.to_be_bytes().as_ref())
                }
                TypedValueRef::SimpleLiteral(v) => builder.append_string(v.value),
                TypedValueRef::LanguageStringLiteral(v) => builder.append_string(v.value),
                TypedValueRef::DateTimeLiteral(v) => builder.append_date_time(v),
                TypedValueRef::TimeLiteral(v) => builder.append_time(v),
                TypedValueRef::DateLiteral(v) => builder.append_date(v),
                TypedValueRef::DurationLiteral(v) => builder.append_duration(v),
                TypedValueRef::YearMonthDurationLiteral(v) => {
                    builder.append_year_month_duration(v)
                }
                TypedValueRef::DayTimeDurationLiteral(v) => builder.append_day_time_duration(v),
                TypedValueRef::OtherLiteral(v) => builder.append_literal(v),
            }
        } else {
            builder.append_null()
        }
    }

    builder.finish()
}
