use std::fmt;

/// An RDF Fusion builtin name.
#[derive(Eq, PartialEq, Debug, Clone, Copy, Hash)]
pub enum BuiltinName {
    // SPARQL Builtin Scalar Functions
    Str,
    Lang,
    LangMatches,
    Datatype,
    Iri,
    BNode,
    Rand,
    Abs,
    Ceil,
    Floor,
    Round,
    Concat,
    SubStr,
    StrLen,
    Replace,
    UCase,
    LCase,
    EncodeForUri,
    Contains,
    StrStarts,
    StrEnds,
    StrBefore,
    StrAfter,
    Year,
    Month,
    Day,
    Hours,
    Minutes,
    Seconds,
    Timezone,
    Tz,
    Uuid,
    StrUuid,
    Md5,
    Sha1,
    Sha256,
    Sha384,
    Sha512,
    StrLang,
    StrDt,
    IsIri,
    IsBlank,
    IsLiteral,
    IsNumeric,
    Regex,
    Bound,
    Coalesce,
    If,

    // Scalar Built-in Aggregate Functions
    Sum,
    Min,
    Max,
    Avg,
    GroupConcat,

    // Comparison functions
    SameTerm,
    Equal,
    GreaterThan,
    GreaterOrEqual,
    LessThan,
    LessOrEqual,

    // Numeric functions
    Add,
    Div,
    Mul,
    Sub,
    UnaryMinus,
    UnaryPlus,

    // Logical
    And,
    Or,
    Not,

    // Conversion Functions
    CastString,
    CastInteger,
    AsInt,
    CastFloat,
    CastDouble,
    CastDecimal,
    CastDateTime,
    CastBoolean,

    // Encoding
    WithSortableEncoding,
    WithTypedValueEncoding,
    WithPlainTermEncoding,

    // Other Necessary Functions
    EffectiveBooleanValue,
    NativeBooleanAsTerm,
    NativeInt64AsTerm,
    IsCompatible,
}

impl fmt::Display for BuiltinName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Str => "STR",
            Self::Lang => "LANG",
            Self::LangMatches => "LANGMATCHES",
            Self::Datatype => "DATATYPE",
            Self::Iri => "IRI",
            Self::BNode => "BNODE",
            Self::Rand => "RAND",
            Self::Abs => "ABS",
            Self::Ceil => "CEIL",
            Self::Floor => "FLOOR",
            Self::Round => "ROUND",
            Self::Concat => "CONCAT",
            Self::SubStr => "SUBSTR",
            Self::StrLen => "STRLEN",
            Self::Replace => "REPLACE",
            Self::UCase => "UCASE",
            Self::LCase => "LCASE",
            Self::EncodeForUri => "ENCODE_FOR_URI",
            Self::Contains => "CONTAINS",
            Self::StrStarts => "STRSTARTS",
            Self::StrEnds => "STRENDS",
            Self::StrBefore => "STRBEFORE",
            Self::StrAfter => "STRAFTER",
            Self::Year => "YEAR",
            Self::Month => "MONTH",
            Self::Day => "DAY",
            Self::Hours => "HOURS",
            Self::Minutes => "MINUTES",
            Self::Seconds => "SECONDS",
            Self::Timezone => "TIMEZONE",
            Self::Tz => "TZ",
            Self::Uuid => "UUID",
            Self::StrUuid => "STRUUID",
            Self::Md5 => "MD5",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
            Self::Sha384 => "SHA384",
            Self::Sha512 => "SHA512",
            Self::StrLang => "STRLANG",
            Self::StrDt => "STRDT",
            Self::IsIri => "isIRI",
            Self::IsBlank => "isBLANK",
            Self::IsLiteral => "isLITERAL",
            Self::IsNumeric => "isNUMERIC",
            Self::Regex => "REGEX",
            Self::If => "IF",
            Self::SameTerm => "SAMETERM",
            Self::Equal => "EQ",
            Self::GreaterThan => "GT",
            Self::GreaterOrEqual => "GEQ",
            Self::LessThan => "LT",
            Self::LessOrEqual => "LEQ",
            Self::Add => "ADD",
            Self::Div => "DIV",
            Self::Mul => "MUL",
            Self::Sub => "SUB",
            Self::CastString => "xsd:string",
            Self::CastInteger => "xsd:integer",
            Self::AsInt => "xsd:int",
            Self::CastFloat => "xsd:float",
            Self::CastDouble => "xsd:double",
            Self::CastDecimal => "xsd:decimal",
            Self::CastDateTime => "xsd:dataTime",
            Self::CastBoolean => "xsd:boolean",
            Self::EffectiveBooleanValue => "EFFECTIVE_BOOLEAN_VALUE",
            Self::NativeBooleanAsTerm => "BOOLEAN_AS_TERM",
            Self::NativeInt64AsTerm => "INT64_AS_TERM",
            Self::Bound => "BOUND",
            Self::IsCompatible => "IS_COMPATIBLE",
            Self::Coalesce => "COALESCE",
            Self::UnaryMinus => "MINUS",
            Self::UnaryPlus => "PLUS",
            Self::Sum => "SUM",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Avg => "AVG",
            Self::GroupConcat => "GROUP_CONCAT",
            Self::WithSortableEncoding => "WITH_SORTABLE_ENCODING",
            Self::WithTypedValueEncoding => "WITH_TYPED_VALUE_ENCODING",
            Self::WithPlainTermEncoding => "WITH_PLAIN_TERM_ENCODING",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Not => "NOT",
        };
        f.write_str(name)
    }
}
