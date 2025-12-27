use crate::typed_value::family::{create_struct_scalar, TypeClaim, TypeFamily};
use datafusion::arrow::array::{
    Array, AsArray, Decimal128Array, Int64Array, StructArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{exec_err, ScalarValue};
use rdf_fusion_model::vocab::xsd;
use rdf_fusion_model::{DFResult, Decimal, TermRef, TypedValueRef};
use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::sync::LazyLock;

/// Family for `xsd:duration`, `xsd:yearMonthDuration` and `xsd:dayTimeDuration`.
///
/// While both arrays are nullable, functions creating values of this type must ensure that for a
/// single value, at least one of the two arrays is valid.
///
/// # Layout
///
/// The layout of the duration family is stored as a struct array with two fields:
/// - `months`: an optional array of month values
/// - `seconds`: an optional array of second values
///
/// Depending on which fields are set, the duration is interpreted as one of the three duration
/// types.
///
/// ┌─────────────────────────────┐
/// │ Struct Array                │
/// │                             │
/// │   Months        Seconds     │
/// │  ┌──────────┐  ┌──────────┐ │
/// │  │ 12       │  │ NULL     │ │
/// │  │──────────│  │──────────│ │
/// │  │ NULL     │  │ 3600     │ │
/// │  │──────────│  │──────────│ │
/// │  │ 24       │  │ 120      │ │
/// │  └──────────┘  └──────────┘ │
/// └─────────────────────────────┘
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct DurationFamily {
    /// The data type of this family.
    data_type: DataType,
    /// The claim of this family.
    claim: TypeClaim,
}

static FIELDS_DURATION: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new("months", DataType::Int64, true),
        Field::new(
            "seconds",
            DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE),
            true,
        ),
    ])
});

impl DurationFamily {
    /// Creates a new [`DurationFamily`].
    pub fn new() -> Self {
        let mut types = BTreeSet::new();
        types.insert(xsd::DURATION.into());
        types.insert(xsd::YEAR_MONTH_DURATION.into());
        types.insert(xsd::DAY_TIME_DURATION.into());
        Self {
            data_type: DataType::Struct(FIELDS_DURATION.clone()),
            claim: TypeClaim::Literal(types),
        }
    }
}

impl TypeFamily for DurationFamily {
    fn id(&self) -> &str {
        "rdf-fusion.duration"
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn claim(&self) -> &TypeClaim {
        &self.claim
    }

    fn encode_value(&self, value: TermRef<'_>) -> DFResult<ScalarValue> {
        let tv = TypedValueRef::try_from(value)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        let (months, seconds) = match tv {
            TypedValueRef::DurationLiteral(d) => {
                (Some(d.year_month()), Some(d.day_time()))
            }
            TypedValueRef::YearMonthDurationLiteral(ym) => (Some(ym), None),
            TypedValueRef::DayTimeDurationLiteral(dt) => (None, Some(dt)),
            _ => return exec_err!("DurationFamily can only encode duration literals"),
        };

        let months_scalar = if let Some(m) = months {
            ScalarValue::Int64(Some(m.as_i64()))
        } else {
            ScalarValue::Int64(None)
        };

        let seconds_scalar = if let Some(s) = seconds {
            let v = i128::from_be_bytes(s.as_seconds().to_be_bytes());
            ScalarValue::Decimal128(Some(v), Decimal::PRECISION, Decimal::SCALE)
        } else {
            ScalarValue::Decimal128(None, Decimal::PRECISION, Decimal::SCALE)
        };

        create_struct_scalar(vec![months_scalar, seconds_scalar], FIELDS_DURATION.clone())
    }
}

impl Debug for DurationFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.id())
    }
}

/// A reference to the child arrays of a [`ResourceFamily`] array.
#[derive(Debug, Clone, Copy)]
pub struct DurationArrayParts<'data> {
    /// The struct array containing the children.
    pub struct_array: &'data StructArray,
    /// The array of months.
    pub months: &'data Int64Array,
    /// The array of seconds.
    pub seconds: &'data Decimal128Array,
}

impl<'data> DurationArrayParts<'data> {
    /// Creates a [`DurationArrayParts`] from the given array.
    ///
    /// Panics if the array does not match the expected schema.
    pub fn from_array(array: &'data dyn Array) -> Self {
        let struct_array = array.as_struct();
        Self {
            struct_array,
            months: struct_array.column(0).as_primitive(),
            seconds: struct_array.column(1).as_primitive(),
        }
    }
}
