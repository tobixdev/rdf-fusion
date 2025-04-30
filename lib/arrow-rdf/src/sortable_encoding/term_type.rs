use model::ThinError;

#[derive(Debug, Clone, Copy)]
pub(super) enum SortableTermType {
    Null,
    BlankNodes,
    NamedNode,
    Boolean,
    Numeric,
    String,
    DateTime,
    Time,
    Date,
    Duration,
    YearMonthDuration,
    DayTimeDuration,
    UnsupportedLiteral,
}

impl SortableTermType {
    pub fn as_u8(self) -> u8 {
        match self {
            SortableTermType::Null => 0,
            SortableTermType::BlankNodes => 1,
            SortableTermType::NamedNode => 2,
            SortableTermType::Boolean => 3,
            SortableTermType::Numeric => 4,
            SortableTermType::String => 5,
            SortableTermType::DateTime => 6,
            SortableTermType::Time => 7,
            SortableTermType::Date => 8,
            SortableTermType::Duration => 9,
            SortableTermType::YearMonthDuration => 10,
            SortableTermType::DayTimeDuration => 11,
            SortableTermType::UnsupportedLiteral => 12,
        }
    }
}

impl TryFrom<u8> for SortableTermType {
    type Error = ThinError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        let term_type = match value {
            0 => SortableTermType::Null,
            1 => SortableTermType::BlankNodes,
            2 => SortableTermType::NamedNode,
            3 => SortableTermType::Boolean,
            4 => SortableTermType::Numeric,
            5 => SortableTermType::String,
            6 => SortableTermType::DateTime,
            7 => SortableTermType::Time,
            8 => SortableTermType::Date,
            9 => SortableTermType::Duration,
            10 => SortableTermType::YearMonthDuration,
            11 => SortableTermType::DayTimeDuration,
            12 => SortableTermType::UnsupportedLiteral,
            _ => return ThinError::internal_error("Invalid value for SortableTermType."),
        };
        Ok(term_type)
    }
}
