use crate::typed_value::family::TypeClaim;
use crate::typed_value::{
    TypedValueArray, TypedValueArrayBuilder, TypedValueEncodingRef,
};
use datafusion::common::ScalarValue;
use rdf_fusion_model::vocab::xsd;
use rdf_fusion_model::{
    AResult, BlankNodeRef, Boolean, Date, DateTime, DayTimeDuration, Decimal, Double,
    Float, Int, Integer, LiteralRef, NamedNodeRef, Numeric, TermRef, Time, TypedValueRef,
    YearMonthDuration,
};

/// Allows creating a [TypedValueArray] element-by-element.
///
/// /// If you aim to build an array based on its individual parts, see
/// [TypedValueArrayBuilder](super::TypedValueArrayBuilder).
pub struct TypedValueArrayElementBuilder {
    encoding: TypedValueEncodingRef,
    type_ids: Vec<i8>,
    offsets: Vec<i32>,
    /// Values for each family, indexed by type_id.
    family_values: Vec<Vec<ScalarValue>>,
}

impl TypedValueArrayElementBuilder {
    /// Creates a new [`TypedValueArrayElementBuilder`].
    pub fn new(encoding: TypedValueEncodingRef) -> Self {
        let num_families = encoding.num_type_families();
        let mut family_values = Vec::with_capacity(num_families + 1);
        for _ in 0..=num_families {
            family_values.push(Vec::new());
        }

        Self {
            encoding,
            type_ids: Vec::new(),
            offsets: Vec::new(),
            family_values,
        }
    }

    pub fn append_typed_value(&mut self, value: TypedValueRef<'_>) -> AResult<()> {
        match value {
            TypedValueRef::NamedNode(v) => self.append_named_node(v),
            TypedValueRef::BlankNode(v) => self.append_blank_node(v),
            TypedValueRef::BooleanLiteral(v) => self.append_boolean(v),
            TypedValueRef::NumericLiteral(v) => self.append_numeric(v),
            TypedValueRef::SimpleLiteral(v) => self.append_string(v.value, None),
            TypedValueRef::LanguageStringLiteral(v) => {
                self.append_string(v.value, Some(v.language))
            }
            TypedValueRef::DateTimeLiteral(v) => self.append_date_time(v),
            TypedValueRef::TimeLiteral(v) => self.append_time(v),
            TypedValueRef::DateLiteral(v) => self.append_date(v),
            TypedValueRef::DurationLiteral(v) => {
                self.append_duration(Some(v.year_month()), Some(v.day_time()))
            }
            TypedValueRef::YearMonthDurationLiteral(v) => {
                self.append_duration(Some(v), None)
            }
            TypedValueRef::DayTimeDurationLiteral(v) => {
                self.append_duration(None, Some(v))
            }
            TypedValueRef::OtherLiteral(v) => self.append_other_literal(v),
        }
    }

    pub fn append_boolean(&mut self, value: Boolean) -> AResult<()> {
        let s = if value.as_bool() { "true" } else { "false" };
        let term = TermRef::Literal(LiteralRef::new_typed_literal(s, xsd::BOOLEAN));
        self.append_term(term)
    }

    pub fn append_named_node(&mut self, value: NamedNodeRef<'_>) -> AResult<()> {
        self.append_term(TermRef::NamedNode(value))
    }

    pub fn append_blank_node(&mut self, value: BlankNodeRef<'_>) -> AResult<()> {
        self.append_term(TermRef::BlankNode(value))
    }

    pub fn append_string(&mut self, value: &str, language: Option<&str>) -> AResult<()> {
        let term = if let Some(lang) = language {
            TermRef::Literal(LiteralRef::new_language_tagged_literal_unchecked(
                value, lang,
            ))
        } else {
            TermRef::Literal(LiteralRef::new_typed_literal(value, xsd::STRING))
        };
        self.append_term(term)
    }

    pub fn append_date_time(&mut self, value: DateTime) -> AResult<()> {
        let s = value.to_string();
        let term = TermRef::Literal(LiteralRef::new_typed_literal(&s, xsd::DATE_TIME));
        self.append_term(term)
    }

    pub fn append_time(&mut self, value: Time) -> AResult<()> {
        let s = value.to_string();
        let term = TermRef::Literal(LiteralRef::new_typed_literal(&s, xsd::TIME));
        self.append_term(term)
    }

    pub fn append_date(&mut self, value: Date) -> AResult<()> {
        let s = value.to_string();
        let term = TermRef::Literal(LiteralRef::new_typed_literal(&s, xsd::DATE));
        self.append_term(term)
    }

    pub fn append_int(&mut self, int: Int) -> AResult<()> {
        let s = int.to_string();
        let term = TermRef::Literal(LiteralRef::new_typed_literal(&s, xsd::INT));
        self.append_term(term)
    }

    pub fn append_integer(&mut self, integer: Integer) -> AResult<()> {
        let s = integer.to_string();
        let term = TermRef::Literal(LiteralRef::new_typed_literal(&s, xsd::INTEGER));
        self.append_term(term)
    }

    pub fn append_float(&mut self, value: Float) -> AResult<()> {
        let s = value.to_string();
        let term = TermRef::Literal(LiteralRef::new_typed_literal(&s, xsd::FLOAT));
        self.append_term(term)
    }

    pub fn append_double(&mut self, value: Double) -> AResult<()> {
        let s = value.to_string();
        let term = TermRef::Literal(LiteralRef::new_typed_literal(&s, xsd::DOUBLE));
        self.append_term(term)
    }

    pub fn append_decimal(&mut self, value: Decimal) -> AResult<()> {
        let s = value.to_string();
        let term = TermRef::Literal(LiteralRef::new_typed_literal(&s, xsd::DECIMAL));
        self.append_term(term)
    }

    pub fn append_numeric(&mut self, value: Numeric) -> AResult<()> {
        match value {
            Numeric::Int(value) => self.append_int(value),
            Numeric::Integer(value) => self.append_integer(value),
            Numeric::Float(value) => self.append_float(value),
            Numeric::Double(value) => self.append_double(value),
            Numeric::Decimal(value) => self.append_decimal(value),
        }
    }

    pub fn append_duration(
        &mut self,
        year_month: Option<YearMonthDuration>,
        day_time: Option<DayTimeDuration>,
    ) -> AResult<()> {
        if year_month.is_none() && day_time.is_none() {
            return Err(datafusion::arrow::error::ArrowError::InvalidArgumentError(
                String::from("One duration component required"),
            )
            .into());
        }

        let (val, iri) = if let Some(ym) = year_month {
            if let Some(dt) = day_time {
                // Combined duration
                let d = rdf_fusion_model::Duration::new(ym.as_i64(), dt.as_seconds())
                    .map_err(|e| {
                        datafusion::arrow::error::ArrowError::InvalidArgumentError(
                            e.to_string(),
                        )
                    })?;
                (d.to_string(), xsd::DURATION)
            } else {
                (ym.to_string(), xsd::YEAR_MONTH_DURATION)
            }
        } else {
            (day_time.unwrap().to_string(), xsd::DAY_TIME_DURATION)
        };

        let term = TermRef::Literal(LiteralRef::new_typed_literal(&val, iri));
        self.append_term(term)
    }

    pub fn append_other_literal(&mut self, literal: LiteralRef<'_>) -> AResult<()> {
        self.append_term(TermRef::Literal(literal))
    }

    pub fn append_null(&mut self) -> AResult<()> {
        let offset = self.family_values[0].len();
        self.family_values[0].push(ScalarValue::Null);
        self.type_ids.push(0);
        self.offsets
            .push(i32::try_from(offset).expect("Offset too large"));
        Ok(())
    }

    /// Creates the final [`TypedValueArray`].
    pub fn finish(self) -> AResult<TypedValueArray> {
        let mut builder = TypedValueArrayBuilder::new(
            self.encoding.clone(),
            self.type_ids,
            self.offsets,
        )?;

        // Handle Nulls (index 0)
        if !self.family_values[0].is_empty() {
            let arr = ScalarValue::iter_to_array(self.family_values[0].clone())?;
            builder = builder.with_nulls(arr);
        }

        // Handle families (index 1..)
        let families = self.encoding.type_families();
        for (i, vals) in self.family_values.iter().enumerate().skip(1) {
            if vals.is_empty() {
                continue;
            }
            let family_idx = i - 1;
            if family_idx >= families.len() {
                break;
            }
            let family = &families[family_idx];

            // For ResourceFamily, we might have ScalarValue::Union which iter_to_array should handle.
            let arr = ScalarValue::iter_to_array(vals.clone())?;
            builder = builder.with_array(family.as_ref(), Some(arr))?;
        }

        Ok(builder.finish()?)
    }

    fn append_term(&mut self, term: TermRef<'_>) -> AResult<()> {
        // Resource Check (always first family after null?)
        // Implicitly assuming ResourceFamily is at index 0 of families (ID 1).
        if term.is_named_node() || term.is_blank_node() {
            let families = self.encoding.type_families();
            if families.is_empty() {
                return Err(datafusion::arrow::error::ArrowError::ComputeError(
                    "No families registered".to_string(),
                )
                .into());
            }
            let fam = &families[0]; // ResourceFamily
            let scalar = fam.encode_value(term)?;
            let offset = self.family_values[1].len();
            self.family_values[1].push(scalar);
            self.type_ids.push(1);
            self.offsets
                .push(i32::try_from(offset).expect("Offset too large"));
            return Ok(());
        }

        if let TermRef::Literal(lit) = term {
            let datatype = lit.datatype();
            let mut found_id = None;
            let families = self.encoding.type_families();

            // Skip ResourceFamily (index 0)
            for (i, fam) in families.iter().enumerate().skip(1) {
                if let TypeClaim::Literal(claims) = fam.claim() {
                    let owned = datatype.into_owned();
                    if claims.contains(&owned) {
                        found_id = Some(i + 1); // ID = index + 1
                        break;
                    }
                }
            }

            // Fallback to UnknownFamily (last one)
            let id = found_id.unwrap_or(families.len() - 1);

            let fam = &families[id - 1];
            let scalar = fam.encode_value(term)?;

            let offset = self.family_values[id].len();
            self.family_values[id].push(scalar);
            self.type_ids.push(id as i8);
            self.offsets
                .push(i32::try_from(offset).expect("Offset too large"));

            Ok(())
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typed_value::encoding::TypedValueEncoding;
    use datafusion::arrow::util::pretty::pretty_format_columns;
    use rdf_fusion_model::{Boolean, Int};
    use std::sync::Arc;
    use crate::EncodingArray;

    #[test]
    fn test_build_mixed_array() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let mut builder = TypedValueArrayElementBuilder::new(encoding);

        builder.append_boolean(Boolean::from(true)).unwrap();
        builder.append_null().unwrap();
        builder.append_boolean(Boolean::from(false)).unwrap();

        let array = builder.finish().unwrap();

        insta::assert_snapshot!(
            pretty_format_columns("col", &[array.into_array_ref()]).unwrap(),
            @r"
        +----------------------------+
        | col                        |
        +----------------------------+
        | {rdf-fusion.boolean=true}  |
        | {null=}                    |
        | {rdf-fusion.boolean=false} |
        +----------------------------+
        "
        );
    }

    #[test]
    fn test_build_mixed_array_types() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let mut builder = TypedValueArrayElementBuilder::new(encoding);

        builder.append_boolean(Boolean::from(true)).unwrap();
        builder.append_string("hello", None).unwrap();
        builder.append_int(Int::new(42)).unwrap();
        builder.append_null().unwrap();

        let array = builder.finish().unwrap();

        insta::assert_snapshot!(
            pretty_format_columns("col", &[array.into_array_ref()]).unwrap(),
            @r"
        +-------------------------------------------------+
        | col                                             |
        +-------------------------------------------------+
        | {rdf-fusion.boolean=true}                       |
        | {rdf-fusion.strings={value: hello, language: }} |
        | {rdf-fusion.numeric={int=42}}                   |
        | {null=}                                         |
        +-------------------------------------------------+
        "
        );
    }
}

