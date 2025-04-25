use crate::oxigraph_memory::encoder::{
    parse_boolean_str, parse_date_str, parse_date_time_str, parse_day_time_duration_str,
    parse_decimal_str, parse_double_str, parse_duration_str, parse_float_str, parse_g_day_str,
    parse_g_month_day_str, parse_g_month_str, parse_g_year_month_str, parse_g_year_str,
    parse_integer_str, parse_time_str, parse_year_month_duration_str,
};
use crate::oxigraph_memory::hash::StrHash;
use crate::oxigraph_memory::small_string::SmallString;
use model::{
    BlankNodeRef, TermRef, GraphNameRef, LiteralRef, NamedNodeRef, NamedOrBlankNodeRef,
    SubjectRef,
};

#[derive(Debug, Clone)]
pub enum EncodedTerm {
    DefaultGraph, // TODO: do we still need it?
    NamedNode {
        iri_id: StrHash,
    },
    NumericalBlankNode {
        id: [u8; 16],
    },
    SmallBlankNode(SmallString),
    BigBlankNode {
        id_id: StrHash,
    },
    SmallStringLiteral(SmallString),
    BigStringLiteral {
        value_id: StrHash,
    },
    SmallSmallLangStringLiteral {
        value: SmallString,
        language: SmallString,
    },
    SmallBigLangStringLiteral {
        value: SmallString,
        language_id: StrHash,
    },
    BigSmallLangStringLiteral {
        value_id: StrHash,
        language: SmallString,
    },
    BigBigLangStringLiteral {
        value_id: StrHash,
        language_id: StrHash,
    },
    SmallTypedLiteral {
        value: SmallString,
        datatype_id: StrHash,
    },
    BigTypedLiteral {
        value_id: StrHash,
        datatype_id: StrHash,
    },
    BooleanLiteral(oxsdatatypes::Boolean),
    FloatLiteral(oxsdatatypes::Float),
    DoubleLiteral(oxsdatatypes::Double),
    IntegerLiteral(oxsdatatypes::Integer),
    DecimalLiteral(oxsdatatypes::Decimal),
    DateTimeLiteral(oxsdatatypes::DateTime),
    TimeLiteral(oxsdatatypes::Time),
    DateLiteral(oxsdatatypes::Date),
    GYearMonthLiteral(oxsdatatypes::GYearMonth),
    GYearLiteral(oxsdatatypes::GYear),
    GMonthDayLiteral(oxsdatatypes::GMonthDay),
    GDayLiteral(oxsdatatypes::GDay),
    GMonthLiteral(oxsdatatypes::GMonth),
    DurationLiteral(oxsdatatypes::Duration),
    YearMonthDurationLiteral(oxsdatatypes::YearMonthDuration),
    DayTimeDurationLiteral(oxsdatatypes::DayTimeDuration),
}

impl PartialEq for EncodedTerm {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::DefaultGraph, Self::DefaultGraph) => true,
            (Self::NamedNode { iri_id: iri_id_a }, Self::NamedNode { iri_id: iri_id_b }) => {
                iri_id_a == iri_id_b
            }
            (Self::NumericalBlankNode { id: id_a }, Self::NumericalBlankNode { id: id_b }) => {
                id_a == id_b
            }
            (Self::SmallBlankNode(id_a), Self::SmallBlankNode(id_b)) => id_a == id_b,
            (Self::BigBlankNode { id_id: id_a }, Self::BigBlankNode { id_id: id_b }) => {
                id_a == id_b
            }
            (Self::SmallStringLiteral(a), Self::SmallStringLiteral(b)) => a == b,
            (
                Self::BigStringLiteral {
                    value_id: value_id_a,
                },
                Self::BigStringLiteral {
                    value_id: value_id_b,
                },
            ) => value_id_a == value_id_b,
            (
                Self::SmallSmallLangStringLiteral {
                    value: value_a,
                    language: language_a,
                },
                Self::SmallSmallLangStringLiteral {
                    value: value_b,
                    language: language_b,
                },
            ) => value_a == value_b && language_a == language_b,
            (
                Self::SmallBigLangStringLiteral {
                    value: value_a,
                    language_id: language_id_a,
                },
                Self::SmallBigLangStringLiteral {
                    value: value_b,
                    language_id: language_id_b,
                },
            ) => value_a == value_b && language_id_a == language_id_b,
            (
                Self::BigSmallLangStringLiteral {
                    value_id: value_id_a,
                    language: language_a,
                },
                Self::BigSmallLangStringLiteral {
                    value_id: value_id_b,
                    language: language_b,
                },
            ) => value_id_a == value_id_b && language_a == language_b,
            (
                Self::BigBigLangStringLiteral {
                    value_id: value_id_a,
                    language_id: language_id_a,
                },
                Self::BigBigLangStringLiteral {
                    value_id: value_id_b,
                    language_id: language_id_b,
                },
            ) => value_id_a == value_id_b && language_id_a == language_id_b,
            (
                Self::SmallTypedLiteral {
                    value: value_a,
                    datatype_id: datatype_id_a,
                },
                Self::SmallTypedLiteral {
                    value: value_b,
                    datatype_id: datatype_id_b,
                },
            ) => value_a == value_b && datatype_id_a == datatype_id_b,
            (
                Self::BigTypedLiteral {
                    value_id: value_id_a,
                    datatype_id: datatype_id_a,
                },
                Self::BigTypedLiteral {
                    value_id: value_id_b,
                    datatype_id: datatype_id_b,
                },
            ) => value_id_a == value_id_b && datatype_id_a == datatype_id_b,
            (Self::BooleanLiteral(a), Self::BooleanLiteral(b)) => a == b,
            (Self::FloatLiteral(a), Self::FloatLiteral(b)) => a.is_identical_with(*b),
            (Self::DoubleLiteral(a), Self::DoubleLiteral(b)) => a.is_identical_with(*b),
            (Self::IntegerLiteral(a), Self::IntegerLiteral(b)) => a.is_identical_with(*b),
            (Self::DecimalLiteral(a), Self::DecimalLiteral(b)) => a.is_identical_with(*b),
            (Self::DateTimeLiteral(a), Self::DateTimeLiteral(b)) => a.is_identical_with(*b),
            (Self::TimeLiteral(a), Self::TimeLiteral(b)) => a.is_identical_with(*b),
            (Self::DateLiteral(a), Self::DateLiteral(b)) => a.is_identical_with(*b),
            (Self::GYearMonthLiteral(a), Self::GYearMonthLiteral(b)) => a.is_identical_with(*b),
            (Self::GYearLiteral(a), Self::GYearLiteral(b)) => a.is_identical_with(*b),
            (Self::GMonthDayLiteral(a), Self::GMonthDayLiteral(b)) => a.is_identical_with(*b),
            (Self::GMonthLiteral(a), Self::GMonthLiteral(b)) => a.is_identical_with(*b),
            (Self::GDayLiteral(a), Self::GDayLiteral(b)) => a.is_identical_with(*b),
            (Self::DurationLiteral(a), Self::DurationLiteral(b)) => a.is_identical_with(*b),
            (Self::YearMonthDurationLiteral(a), Self::YearMonthDurationLiteral(b)) => {
                a.is_identical_with(*b)
            }
            (Self::DayTimeDurationLiteral(a), Self::DayTimeDurationLiteral(b)) => {
                a.is_identical_with(*b)
            }
            (_, _) => false,
        }
    }
}

impl Eq for EncodedTerm {}

impl EncodedTerm {
    pub fn is_named_node(&self) -> bool {
        matches!(self, Self::NamedNode { .. })
    }

    pub fn is_blank_node(&self) -> bool {
        matches!(
            self,
            Self::NumericalBlankNode { .. }
                | Self::SmallBlankNode { .. }
                | Self::BigBlankNode { .. }
        )
    }

    pub fn is_literal(&self) -> bool {
        matches!(
            self,
            Self::SmallStringLiteral { .. }
                | Self::BigStringLiteral { .. }
                | Self::SmallSmallLangStringLiteral { .. }
                | Self::SmallBigLangStringLiteral { .. }
                | Self::BigSmallLangStringLiteral { .. }
                | Self::BigBigLangStringLiteral { .. }
                | Self::SmallTypedLiteral { .. }
                | Self::BigTypedLiteral { .. }
                | Self::BooleanLiteral(_)
                | Self::FloatLiteral(_)
                | Self::DoubleLiteral(_)
                | Self::IntegerLiteral(_)
                | Self::DecimalLiteral(_)
                | Self::DateTimeLiteral(_)
                | Self::TimeLiteral(_)
                | Self::DateLiteral(_)
                | Self::GYearMonthLiteral(_)
                | Self::GYearLiteral(_)
                | Self::GMonthDayLiteral(_)
                | Self::GDayLiteral(_)
                | Self::GMonthLiteral(_)
                | Self::DurationLiteral(_)
                | Self::YearMonthDurationLiteral(_)
                | Self::DayTimeDurationLiteral(_)
        )
    }

    pub fn is_unknown_typed_literal(&self) -> bool {
        matches!(
            self,
            Self::SmallTypedLiteral { .. } | Self::BigTypedLiteral { .. }
        )
    }

    pub fn is_default_graph(&self) -> bool {
        matches!(self, Self::DefaultGraph)
    }
}

impl From<bool> for EncodedTerm {
    fn from(value: bool) -> Self {
        Self::BooleanLiteral(value.into())
    }
}

impl From<i64> for EncodedTerm {
    fn from(value: i64) -> Self {
        Self::IntegerLiteral(value.into())
    }
}

impl From<i32> for EncodedTerm {
    fn from(value: i32) -> Self {
        Self::IntegerLiteral(value.into())
    }
}

impl From<u32> for EncodedTerm {
    fn from(value: u32) -> Self {
        Self::IntegerLiteral(value.into())
    }
}

impl From<u8> for EncodedTerm {
    fn from(value: u8) -> Self {
        Self::IntegerLiteral(value.into())
    }
}

impl From<f32> for EncodedTerm {
    fn from(value: f32) -> Self {
        Self::FloatLiteral(value.into())
    }
}

impl From<oxsdatatypes::Float> for EncodedTerm {
    fn from(value: oxsdatatypes::Float) -> Self {
        Self::FloatLiteral(value)
    }
}

impl From<f64> for EncodedTerm {
    fn from(value: f64) -> Self {
        Self::DoubleLiteral(value.into())
    }
}

impl From<oxsdatatypes::Boolean> for EncodedTerm {
    fn from(value: oxsdatatypes::Boolean) -> Self {
        Self::BooleanLiteral(value)
    }
}

impl From<oxsdatatypes::Double> for EncodedTerm {
    fn from(value: oxsdatatypes::Double) -> Self {
        Self::DoubleLiteral(value)
    }
}

impl From<oxsdatatypes::Integer> for EncodedTerm {
    fn from(value: oxsdatatypes::Integer) -> Self {
        Self::IntegerLiteral(value)
    }
}

impl From<oxsdatatypes::Decimal> for EncodedTerm {
    fn from(value: oxsdatatypes::Decimal) -> Self {
        Self::DecimalLiteral(value)
    }
}

impl From<oxsdatatypes::DateTime> for EncodedTerm {
    fn from(value: oxsdatatypes::DateTime) -> Self {
        Self::DateTimeLiteral(value)
    }
}

impl From<oxsdatatypes::Time> for EncodedTerm {
    fn from(value: oxsdatatypes::Time) -> Self {
        Self::TimeLiteral(value)
    }
}

impl From<oxsdatatypes::Date> for EncodedTerm {
    fn from(value: oxsdatatypes::Date) -> Self {
        Self::DateLiteral(value)
    }
}

impl From<oxsdatatypes::GMonthDay> for EncodedTerm {
    fn from(value: oxsdatatypes::GMonthDay) -> Self {
        Self::GMonthDayLiteral(value)
    }
}

impl From<oxsdatatypes::GDay> for EncodedTerm {
    fn from(value: oxsdatatypes::GDay) -> Self {
        Self::GDayLiteral(value)
    }
}

impl From<oxsdatatypes::GMonth> for EncodedTerm {
    fn from(value: oxsdatatypes::GMonth) -> Self {
        Self::GMonthLiteral(value)
    }
}

impl From<oxsdatatypes::GYearMonth> for EncodedTerm {
    fn from(value: oxsdatatypes::GYearMonth) -> Self {
        Self::GYearMonthLiteral(value)
    }
}

impl From<oxsdatatypes::GYear> for EncodedTerm {
    fn from(value: oxsdatatypes::GYear) -> Self {
        Self::GYearLiteral(value)
    }
}

impl From<oxsdatatypes::Duration> for EncodedTerm {
    fn from(value: oxsdatatypes::Duration) -> Self {
        Self::DurationLiteral(value)
    }
}

impl From<oxsdatatypes::YearMonthDuration> for EncodedTerm {
    fn from(value: oxsdatatypes::YearMonthDuration) -> Self {
        Self::YearMonthDurationLiteral(value)
    }
}

impl From<oxsdatatypes::DayTimeDuration> for EncodedTerm {
    fn from(value: oxsdatatypes::DayTimeDuration) -> Self {
        Self::DayTimeDurationLiteral(value)
    }
}

impl From<NamedNodeRef<'_>> for EncodedTerm {
    fn from(named_node: NamedNodeRef<'_>) -> Self {
        Self::NamedNode {
            iri_id: StrHash::new(named_node.as_str()),
        }
    }
}

impl From<BlankNodeRef<'_>> for EncodedTerm {
    fn from(blank_node: BlankNodeRef<'_>) -> Self {
        if let Some(id) = blank_node.unique_id() {
            Self::NumericalBlankNode {
                id: id.to_be_bytes(),
            }
        } else {
            let id = blank_node.as_str();
            if let Ok(id) = id.try_into() {
                Self::SmallBlankNode(id)
            } else {
                Self::BigBlankNode {
                    id_id: StrHash::new(id),
                }
            }
        }
    }
}

impl From<LiteralRef<'_>> for EncodedTerm {
    fn from(literal: LiteralRef<'_>) -> Self {
        let value = literal.value();
        let datatype = literal.datatype().as_str();
        let native_encoding = match datatype {
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString" => {
                literal.language().map(|language| {
                    if let Ok(value) = SmallString::try_from(value) {
                        if let Ok(language) = SmallString::try_from(language) {
                            Self::SmallSmallLangStringLiteral { value, language }
                        } else {
                            Self::SmallBigLangStringLiteral {
                                value,
                                language_id: StrHash::new(language),
                            }
                        }
                    } else if let Ok(language) = SmallString::try_from(language) {
                        Self::BigSmallLangStringLiteral {
                            value_id: StrHash::new(value),
                            language,
                        }
                    } else {
                        Self::BigBigLangStringLiteral {
                            value_id: StrHash::new(value),
                            language_id: StrHash::new(language),
                        }
                    }
                })
            }
            "http://www.w3.org/2001/XMLSchema#boolean" => parse_boolean_str(value),
            "http://www.w3.org/2001/XMLSchema#string" => {
                Some(if let Ok(value) = SmallString::try_from(value) {
                    Self::SmallStringLiteral(value)
                } else {
                    Self::BigStringLiteral {
                        value_id: StrHash::new(value),
                    }
                })
            }
            "http://www.w3.org/2001/XMLSchema#float" => parse_float_str(value),
            "http://www.w3.org/2001/XMLSchema#double" => parse_double_str(value),
            "http://www.w3.org/2001/XMLSchema#integer"
            | "http://www.w3.org/2001/XMLSchema#byte"
            | "http://www.w3.org/2001/XMLSchema#short"
            | "http://www.w3.org/2001/XMLSchema#int"
            | "http://www.w3.org/2001/XMLSchema#long"
            | "http://www.w3.org/2001/XMLSchema#unsignedByte"
            | "http://www.w3.org/2001/XMLSchema#unsignedShort"
            | "http://www.w3.org/2001/XMLSchema#unsignedInt"
            | "http://www.w3.org/2001/XMLSchema#unsignedLong"
            | "http://www.w3.org/2001/XMLSchema#positiveInteger"
            | "http://www.w3.org/2001/XMLSchema#negativeInteger"
            | "http://www.w3.org/2001/XMLSchema#nonPositiveInteger"
            | "http://www.w3.org/2001/XMLSchema#nonNegativeInteger" => parse_integer_str(value),
            "http://www.w3.org/2001/XMLSchema#decimal" => parse_decimal_str(value),
            "http://www.w3.org/2001/XMLSchema#dateTime"
            | "http://www.w3.org/2001/XMLSchema#dateTimeStamp" => parse_date_time_str(value),
            "http://www.w3.org/2001/XMLSchema#time" => parse_time_str(value),
            "http://www.w3.org/2001/XMLSchema#date" => parse_date_str(value),
            "http://www.w3.org/2001/XMLSchema#gYearMonth" => parse_g_year_month_str(value),
            "http://www.w3.org/2001/XMLSchema#gYear" => parse_g_year_str(value),
            "http://www.w3.org/2001/XMLSchema#gMonthDay" => parse_g_month_day_str(value),
            "http://www.w3.org/2001/XMLSchema#gDay" => parse_g_day_str(value),
            "http://www.w3.org/2001/XMLSchema#gMonth" => parse_g_month_str(value),
            "http://www.w3.org/2001/XMLSchema#duration" => parse_duration_str(value),
            "http://www.w3.org/2001/XMLSchema#yearMonthDuration" => {
                parse_year_month_duration_str(value)
            }
            "http://www.w3.org/2001/XMLSchema#dayTimeDuration" => {
                parse_day_time_duration_str(value)
            }
            _ => None,
        };
        match native_encoding {
            Some(term) => term,
            None => {
                if let Ok(value) = SmallString::try_from(value) {
                    Self::SmallTypedLiteral {
                        value,
                        datatype_id: StrHash::new(datatype),
                    }
                } else {
                    Self::BigTypedLiteral {
                        value_id: StrHash::new(value),
                        datatype_id: StrHash::new(datatype),
                    }
                }
            }
        }
    }
}

impl From<NamedOrBlankNodeRef<'_>> for EncodedTerm {
    fn from(term: NamedOrBlankNodeRef<'_>) -> Self {
        match term {
            NamedOrBlankNodeRef::NamedNode(named_node) => named_node.into(),
            NamedOrBlankNodeRef::BlankNode(blank_node) => blank_node.into(),
        }
    }
}

impl From<SubjectRef<'_>> for EncodedTerm {
    fn from(term: SubjectRef<'_>) -> Self {
        match term {
            SubjectRef::NamedNode(named_node) => named_node.into(),
            SubjectRef::BlankNode(blank_node) => blank_node.into(),
        }
    }
}

impl From<TermRef<'_>> for EncodedTerm {
    fn from(term: TermRef<'_>) -> Self {
        match term {
            TermRef::NamedNode(named_node) => named_node.into(),
            TermRef::BlankNode(blank_node) => blank_node.into(),
            TermRef::Literal(literal) => literal.into(),
        }
    }
}

impl From<GraphNameRef<'_>> for EncodedTerm {
    fn from(name: GraphNameRef<'_>) -> Self {
        match name {
            GraphNameRef::NamedNode(named_node) => named_node.into(),
            GraphNameRef::BlankNode(blank_node) => blank_node.into(),
            GraphNameRef::DefaultGraph => Self::DefaultGraph,
        }
    }
}
