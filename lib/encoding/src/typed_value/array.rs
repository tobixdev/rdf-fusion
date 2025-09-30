use crate::TermEncoding;
use crate::encoding::EncodingArray;
use crate::typed_value::{
    TYPED_VALUE_ENCODING, TypedValueEncoding, TypedValueEncodingField,
};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Float32Array, Float64Array,
    GenericStringArray, Int16Array, Int32Array, Int64Array, UnionArray,
};
use datafusion::arrow::datatypes::{
    Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
};
use datafusion::common::{DataFusionError, exec_err};

/// Represents an Arrow array with a [TypedValueEncoding].
#[derive(Clone)]
pub struct TypedValueArray {
    inner: ArrayRef,
}

impl TypedValueArray {
    /// Creates a new [TypedValueArray] without verifying the schema.
    pub(super) fn new_unchecked(array: ArrayRef) -> Self {
        Self { inner: array }
    }
}

impl TypedValueArray {
    /// Returns a reference to all the child arrays contained in this array. It is expected to call
    /// this method once and work on the resulting [TypedValueArrayParts].
    ///
    /// Using this has multiple benefits:
    /// - Can reduce runtime checks for accessing and downcasting child arrays
    /// - A bit more static guarantees, as the struct will change if a child array is removed
    pub fn parts_as_ref(&self) -> TypedValueArrayParts<'_> {
        let array = self.inner.as_union();
        let strings_array = array
            .child(TypedValueEncodingField::String.type_id())
            .as_struct();
        let date_times_array = array
            .child(TypedValueEncodingField::DateTime.type_id())
            .as_struct();
        let times_array = array
            .child(TypedValueEncodingField::Time.type_id())
            .as_struct();
        let dates_array = array
            .child(TypedValueEncodingField::Date.type_id())
            .as_struct();
        let durations_array = array
            .child(TypedValueEncodingField::Duration.type_id())
            .as_struct();
        let other_literals_array = array
            .child(TypedValueEncodingField::OtherLiteral.type_id())
            .as_struct();

        TypedValueArrayParts {
            array,
            null_count: array.child(TypedValueEncodingField::Null.type_id()).len(),
            named_nodes: array
                .child(TypedValueEncodingField::NamedNode.type_id())
                .as_string::<i32>(),
            blank_nodes: array
                .child(TypedValueEncodingField::BlankNode.type_id())
                .as_string::<i32>(),
            strings: StringParts {
                value: strings_array.column(0).as_string::<i32>(),
                language: strings_array.column(1).as_string::<i32>(),
            },
            booleans: array
                .child(TypedValueEncodingField::Boolean.type_id())
                .as_boolean(),
            floats: array
                .child(TypedValueEncodingField::Float.type_id())
                .as_primitive::<Float32Type>(),
            doubles: array
                .child(TypedValueEncodingField::Double.type_id())
                .as_primitive::<Float64Type>(),
            decimals: array
                .child(TypedValueEncodingField::Decimal.type_id())
                .as_primitive::<Decimal128Type>(),
            ints: array
                .child(TypedValueEncodingField::Int.type_id())
                .as_primitive::<Int32Type>(),
            integers: array
                .child(TypedValueEncodingField::Integer.type_id())
                .as_primitive::<Int64Type>(),
            date_times: TimestampParts {
                value: date_times_array.column(0).as_primitive::<Decimal128Type>(),
                offset: date_times_array.column(1).as_primitive::<Int16Type>(),
            },
            times: TimestampParts {
                value: times_array.column(0).as_primitive::<Decimal128Type>(),
                offset: times_array.column(1).as_primitive::<Int16Type>(),
            },
            dates: TimestampParts {
                value: dates_array.column(0).as_primitive::<Decimal128Type>(),
                offset: dates_array.column(1).as_primitive::<Int16Type>(),
            },
            durations: DurationParts {
                months: durations_array.column(0).as_primitive::<Int64Type>(),
                seconds: durations_array.column(1).as_primitive::<Decimal128Type>(),
            },
            other_literals: OtherLiteralParts {
                value: other_literals_array.column(0).as_string::<i32>(),
                datatype: other_literals_array.column(1).as_string::<i32>(),
            },
        }
    }
}

impl TryFrom<ArrayRef> for TypedValueArray {
    type Error = DataFusionError;

    fn try_from(value: ArrayRef) -> Result<Self, Self::Error> {
        if value.data_type() != &TYPED_VALUE_ENCODING.data_type() {
            return exec_err!("Unexpected type when creating a value-encoded array");
        }
        Ok(Self { inner: value })
    }
}

impl EncodingArray for TypedValueArray {
    type Encoding = TypedValueEncoding;

    fn encoding(&self) -> &Self::Encoding {
        &TYPED_VALUE_ENCODING
    }

    fn array(&self) -> &ArrayRef {
        &self.inner
    }

    fn into_array(self) -> ArrayRef {
        self.inner
    }
}

#[derive(Debug, Clone)]
pub struct TypedValueArrayParts<'data> {
    pub array: &'data UnionArray,
    pub null_count: usize,
    pub named_nodes: &'data GenericStringArray<i32>,
    pub blank_nodes: &'data GenericStringArray<i32>,
    pub strings: StringParts<'data>,
    pub booleans: &'data BooleanArray,
    pub floats: &'data Float32Array,
    pub doubles: &'data Float64Array,
    pub decimals: &'data Decimal128Array,
    pub ints: &'data Int32Array,
    pub integers: &'data Int64Array,
    pub date_times: TimestampParts<'data>,
    pub times: TimestampParts<'data>,
    pub dates: TimestampParts<'data>,
    pub durations: DurationParts<'data>,
    pub other_literals: OtherLiteralParts<'data>,
}

#[derive(Debug, Clone, Copy)]
pub struct StringParts<'data> {
    pub value: &'data GenericStringArray<i32>,
    pub language: &'data GenericStringArray<i32>,
}

#[derive(Debug, Clone, Copy)]
pub struct TimestampParts<'data> {
    pub value: &'data Decimal128Array,
    pub offset: &'data Int16Array,
}

#[derive(Debug, Clone, Copy)]
pub struct DurationParts<'data> {
    pub months: &'data Int64Array,
    pub seconds: &'data Decimal128Array,
}

#[derive(Debug, Clone, Copy)]
pub struct OtherLiteralParts<'data> {
    pub value: &'data GenericStringArray<i32>,
    pub datatype: &'data GenericStringArray<i32>,
}
