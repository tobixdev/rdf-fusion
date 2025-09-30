use crate::encoding::TermEncoder;
use crate::sortable_term::{
    SORTABLE_TERM_ENCODING, SortableTermArrayBuilder, SortableTermEncoding,
};
use crate::{EncodingArray, TermEncoding};
use rdf_fusion_model::DFResult;
use rdf_fusion_model::{ThinResult, TypedValueRef};

#[derive(Debug)]
pub struct TypedValueRefSortableTermEncoder;

impl TermEncoder<SortableTermEncoding> for TypedValueRefSortableTermEncoder {
    type Term<'data> = TypedValueRef<'data>;

    fn encode_terms<'data>(
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<<SortableTermEncoding as TermEncoding>::Array> {
        let iter = terms.into_iter();
        let (min, _) = iter.size_hint();
        let mut builder = SortableTermArrayBuilder::new(min);

        for term in iter {
            if let Ok(term) = term {
                match term {
                    TypedValueRef::NamedNode(v) => builder.append_named_node(v),
                    TypedValueRef::BlankNode(v) => builder.append_blank_node(v),
                    TypedValueRef::BooleanLiteral(v) => builder.append_boolean(v),
                    TypedValueRef::NumericLiteral(v) => {
                        builder.append_numeric(v, v.to_be_bytes().as_ref())
                    }
                    TypedValueRef::SimpleLiteral(v) => builder.append_string(v.value),
                    TypedValueRef::LanguageStringLiteral(v) => {
                        builder.append_string(v.value)
                    }
                    TypedValueRef::DateTimeLiteral(v) => builder.append_date_time(v),
                    TypedValueRef::TimeLiteral(v) => builder.append_time(v),
                    TypedValueRef::DateLiteral(v) => builder.append_date(v),
                    TypedValueRef::DurationLiteral(v) => builder.append_duration(v),
                    TypedValueRef::YearMonthDurationLiteral(v) => {
                        builder.append_year_month_duration(v)
                    }
                    TypedValueRef::DayTimeDurationLiteral(v) => {
                        builder.append_day_time_duration(v)
                    }
                    TypedValueRef::OtherLiteral(v) => builder.append_literal(v),
                }
            } else {
                builder.append_null()
            }
        }

        SORTABLE_TERM_ENCODING.try_new_array(builder.finish())
    }

    fn encode_term(
        term: ThinResult<Self::Term<'_>>,
    ) -> DFResult<<SortableTermEncoding as TermEncoding>::Scalar> {
        Self::encode_terms([term])?.try_as_scalar(0)
    }
}
