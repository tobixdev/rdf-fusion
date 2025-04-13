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
    pub fn as_u8(&self) -> u8 {
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
