use crate::encoded::dispatch::dispatch_unary;
use crate::encoded::write_enc_term::WriteEncTerm;
use crate::encoded::EncTerm;
use crate::{
    make_binary_rdf_udf, make_n_ary_rdf_udf, make_nullary_rdf_udf, make_quaternary_rdf_udf,
    make_ternary_rdf_udf, make_unary_rdf_udf,
};
use datafusion::logical_expr::ScalarUDF;
use datamodel::Iri;
use functions_scalar::*;

// Comparison

make_binary_rdf_udf!(SameTermRdfOp, EncSameTerm, ENC_SAME_TERM, "enc_same_term");
make_binary_rdf_udf!(EqRdfOp, EncEq, ENC_EQ, "enc_eq");
make_binary_rdf_udf!(
    GreaterThanRdfOp,
    EncGreaterThan,
    ENC_GREATER_THAN,
    "enc_greater_than"
);
make_binary_rdf_udf!(
    GreaterOrEqualRdfOp,
    EncGreaterOrEqual,
    ENC_GREATER_OR_EQUAL,
    "enc_greater_or_equal"
);
make_binary_rdf_udf!(LessThanRdfOp, EncLessThan, ENC_LESS_THAN, "enc_less_than");
make_binary_rdf_udf!(
    LessOrEqualRdfOp,
    EncLessOrEqual,
    ENC_LESS_OR_EQUAL,
    "enc_less_or_equal"
);

// Conversion

make_unary_rdf_udf!(
    AsBooleanRdfOp,
    EncAsBoolean,
    ENC_AS_BOOLEAN,
    "enc_as_boolean"
);
make_unary_rdf_udf!(
    AsDecimalRdfOp,
    EncAsDecimal,
    ENC_AS_DECIMAL,
    "enc_as_decimal"
);
make_unary_rdf_udf!(AsDoubleRdfOp, EncAsDouble, ENC_AS_DOUBLE, "enc_as_double");
make_unary_rdf_udf!(AsFloatRdfOp, EncAsFloat, ENC_AS_FLOAT, "enc_as_float");
make_unary_rdf_udf!(AsIntRdfOp, EncAsInt, ENC_AS_INT, "enc_as_int");
make_unary_rdf_udf!(
    AsIntegerRdfOp,
    EncAsInteger,
    ENC_AS_INTEGER,
    "enc_as_integer"
);
make_unary_rdf_udf!(
    AsDateTimeRdfOp,
    EncAsDateTimeRdfOp,
    ENC_AS_DATETIME,
    "enc_as_datetime"
);
make_unary_rdf_udf!(AsStringRdfOp, EncAsString, ENC_AS_STRING, "enc_as_string");

// Functional Forms

make_unary_rdf_udf!(BoundRdfOp, EncBound, ENC_BOUND, "enc_bound");
make_n_ary_rdf_udf!(CoalesceRdfOp, EncCoalesce, ENC_COALESCE, "enc_coalesce");

// Numeric

make_unary_rdf_udf!(AbsRdfOp, EncAbs, ENC_ABS, "enc_abs");
make_binary_rdf_udf!(AddRdfOp, EncAdd, ENC_ADD, "enc_add");
make_binary_rdf_udf!(DivRdfOp, EncDiv, ENC_DIV, "enc_div");
make_binary_rdf_udf!(MulRdfOp, EncMul, ENC_MUL, "enc_mul");
make_binary_rdf_udf!(SubRdfOp, EncSub, ENC_SUB, "enc_sub");
make_unary_rdf_udf!(
    UnaryMinusRdfOp,
    EncUnaryMinus,
    ENC_UNARY_MINUS,
    "enc_unary_minus"
);
make_unary_rdf_udf!(
    UnaryPlusRdfOp,
    EncUnaryPlus,
    ENC_UNARY_PLUS,
    "enc_unary_plus"
);
make_unary_rdf_udf!(RoundRdfOp, EncRound, ENC_ROUND, "enc_round");
make_unary_rdf_udf!(CeilRdfOp, EncCeil, ENC_CEIL, "enc_ceil");
make_unary_rdf_udf!(FloorRdfOp, EncFloor, ENC_FLOOR, "enc_floor");
make_nullary_rdf_udf!(RandRdfOp, EncRand, ENC_RAND, "enc_rand");

// Strings

make_unary_rdf_udf!(StrLenRdfOp, EncStrlen, ENC_STRLEN, "enc_strlen");
make_binary_rdf_udf!(SubStrRdfOp, EncSubstr, ENC_SUBSTR, "enc_substr");
make_unary_rdf_udf!(UCaseRdfOp, EncUcase, ENC_UCASE, "enc_ucase");
make_unary_rdf_udf!(LCaseRdfOp, EncLcase, ENC_LCASE, "enc_lcase");
make_binary_rdf_udf!(StrStartsRdfOp, EncStrstarts, ENC_STRSTARTS, "enc_strstarts");
make_binary_rdf_udf!(StrEndsRdfOp, EncStrends, ENC_STRENDS, "enc_strends");
make_n_ary_rdf_udf!(ConcatRdfOp, EncConcat, ENC_CONCAT, "enc_concat");
make_binary_rdf_udf!(ContainsRdfOp, EncContains, ENC_CONTAINS, "enc_contains");
make_binary_rdf_udf!(StrBeforeRdfOp, EncStrbefore, ENC_STRBEFORE, "enc_strbefore");
make_binary_rdf_udf!(StrAfterRdfOp, EncStrafter, ENC_STRAFTER, "enc_strafter");
make_unary_rdf_udf!(
    EncodeForUriRdfOp,
    EncEncodeforuri,
    ENC_ENCODEFORURI,
    "enc_encodeforuri"
);
make_binary_rdf_udf!(
    LangMatchesRdfOp,
    EncLangmatches,
    ENC_LANGMATCHES,
    "enc_langmatches"
);
make_binary_rdf_udf!(RegexRdfOp, EncRegexBinary, ENC_REGEX_BINARY, "enc_regex");
make_ternary_rdf_udf!(RegexRdfOp, EncRegexTernary, ENC_REGEX_TERNARY, "enc_regex");
make_ternary_rdf_udf!(
    ReplaceRdfOp,
    EncReplaceTernary,
    ENC_REPLACE_TERNARY,
    "enc_replace"
);
make_quaternary_rdf_udf!(
    ReplaceRdfOp,
    EncReplaceQuaternary,
    ENC_REPLACE_QUATERNARY,
    "enc_replace"
);

// Terms

make_unary_rdf_udf!(DatatypeRdfOp, EncDatatype, ENC_DATATYPE, "enc_datatype");
make_unary_rdf_udf!(IsIriRdfOp, EncIsIri, ENC_IS_IRI, "enc_is_iri");
make_unary_rdf_udf!(IsBlankRdfOp, EncIsBlank, ENC_IS_BLANK, "enc_is_blank");
make_unary_rdf_udf!(
    IsLiteralRdfOp,
    EncIsLiteral,
    ENC_IS_LITERAL,
    "enc_is_literal"
);
make_unary_rdf_udf!(
    IsNumericRdfOp,
    EncIsNumeric,
    ENC_IS_NUMERIC,
    "enc_is_numeric"
);
make_unary_rdf_udf!(StrRdfOp, EncStr, ENC_STR, "enc_str");
make_unary_rdf_udf!(LangRdfOp, EncLang, ENC_LANG, "enc_lang");
make_unary_rdf_udf!(BNodeRdfOp, EncBnodeNullary, ENC_BNODE_NULLARY, "enc_bnode");
make_unary_rdf_udf!(BNodeRdfOp, EncBnodeUnary, ENC_BNODE_UNARY, "enc_bnode");
make_binary_rdf_udf!(StrDtRdfOp, EncStrdt, ENC_STRDT, "enc_strdt");
make_binary_rdf_udf!(StrLangRdfOp, EncStrlang, ENC_STRLANG, "enc_strlang");
make_nullary_rdf_udf!(UuidRdfOp, EncUuid, ENC_UUID, "enc_uuid");
make_nullary_rdf_udf!(StrUuidRdfOp, EncStruuid, ENC_STRUUID, "enc_struuid");


// Date and Time

make_unary_rdf_udf!(YearRdfOp, EncYear, ENC_YEAR, "enc_year");
make_unary_rdf_udf!(MonthRdfOp, EncMonth, ENC_MONTH, "enc_month");
make_unary_rdf_udf!(DayRdfOp, EncDay, ENC_DAY, "enc_day");
make_unary_rdf_udf!(HoursRdfOp, EncHours, ENC_HOURS, "enc_hours");
make_unary_rdf_udf!(MinutesRdfOp, EncMinutes, ENC_MINUTES, "enc_minutes");
make_unary_rdf_udf!(SecondsRdfOp, EncSeconds, ENC_SECONDS, "enc_seconds");
make_unary_rdf_udf!(TimezoneRdfOp, EncTimezone, ENC_TIMEZONE, "enc_timezone");
make_unary_rdf_udf!(TzRdfOp, EncTz, ENC_TZ, "enc_tz");

// Hash

make_unary_rdf_udf!(Md5RdfOp, EncMd5, ENC_MD5, "enc_md5");
make_unary_rdf_udf!(Sha1RdfOp, EncSha1, ENC_SHA1, "enc_sha1");
make_unary_rdf_udf!(Sha256RdfOp, EncSha256, ENC_SHA256, "enc_sha256");
make_unary_rdf_udf!(Sha384RdfOp, EncSha384, ENC_SHA384, "enc_sha384");
make_unary_rdf_udf!(Sha512RdfOp, EncSha512, ENC_SHA512, "enc_sha512");

#[derive(Debug)]
pub struct EncIri {
    signature: datafusion::logical_expr::Signature,
    implementation: IriRdfOp,
}

impl EncIri {
    pub fn new(base_iri: Option<Iri<String>>) -> Self {
        Self {
            signature: datafusion::logical_expr::Signature::new(
                datafusion::logical_expr::TypeSignature::Exact(vec![EncTerm::term_type()]),
                datafusion::logical_expr::Volatility::Immutable,
            ),
            implementation: IriRdfOp::new(base_iri),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for EncIri {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "enc_iri"
    }

    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[datafusion::arrow::datatypes::DataType],
    ) -> crate::DFResult<datafusion::arrow::datatypes::DataType> {
        Ok(EncTerm::term_type())
    }

    fn invoke_batch(
        &self,
        args: &[datafusion::physical_plan::ColumnarValue],
        number_rows: usize,
    ) -> datafusion::common::Result<datafusion::physical_plan::ColumnarValue> {
        dispatch_unary(&self.implementation, args, number_rows)
    }
}

pub fn enc_iri(base_iri: Option<Iri<String>>) -> ScalarUDF {
    ScalarUDF::from(EncIri::new(base_iri))
}
