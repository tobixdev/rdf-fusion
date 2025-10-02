use crate::TermEncoding;
use crate::typed_value::{
    TYPED_VALUE_ENCODING, TypedValueArray, TypedValueEncoding, TypedValueEncodingField,
};
use datafusion::arrow::array::{
    Array, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int32Array,
    Int64Array, NullArray, StringArray, StructArray, UnionArray,
};
use datafusion::arrow::buffer::{NullBuffer, ScalarBuffer};
use datafusion::common::{exec_datafusion_err, exec_err};
use rdf_fusion_model::DFResult;
use std::sync::Arc;

/// Allows creating a [TypedValueArray] from its array parts.
///
/// If you aim to build an array element-by-element, see [TypedValueArrayElementBuilder](super::TypedValueArrayElementBuilder).
pub struct TypedValueArrayBuilder {
    type_ids: Vec<i8>,
    offsets: Vec<i32>,
    named_nodes: Option<Arc<dyn Array>>,
    blank_nodes: Option<Arc<dyn Array>>,
    strings: Option<Arc<dyn Array>>,
    booleans: Option<Arc<dyn Array>>,
    floats: Option<Arc<dyn Array>>,
    doubles: Option<Arc<dyn Array>>,
    decimals: Option<Arc<dyn Array>>,
    ints: Option<Arc<dyn Array>>,
    integers: Option<Arc<dyn Array>>,
    date_times: Option<Arc<dyn Array>>,
    times: Option<Arc<dyn Array>>,
    dates: Option<Arc<dyn Array>>,
    durations: Option<Arc<dyn Array>>,
    typed_literals: Option<Arc<dyn Array>>,
    nulls: Option<Arc<dyn Array>>,
}

impl TypedValueArrayBuilder {
    /// Creates a new [TypedValueArrayBuilder] with the given `type_ids` and `offsets`.
    ///
    /// Use the `with_` methods to assemble the rest of the array.
    ///
    /// # Errors
    ///
    /// Returns an error if the length of the given arrays does not match.
    pub fn new(type_ids: Vec<i8>, offsets: Vec<i32>) -> DFResult<Self> {
        if type_ids.len() != offsets.len() {
            return exec_err!(
                "Length of type_ids and offsets do not match: {} != {}",
                type_ids.len(),
                offsets.len()
            );
        }

        Ok(Self {
            type_ids,
            offsets,
            named_nodes: None,
            blank_nodes: None,
            strings: None,
            booleans: None,
            floats: None,
            doubles: None,
            decimals: None,
            ints: None,
            integers: None,
            date_times: None,
            times: None,
            dates: None,
            durations: None,
            typed_literals: None,
            nulls: None,
        })
    }

    /// Creates a new [TypedValueArrayBuilder] that will only have a single sub array with values.
    pub fn new_with_single_type(type_id: i8, len: usize) -> DFResult<Self> {
        let type_ids = vec![type_id; len];

        let len = i32::try_from(len)
            .map_err(|_| exec_datafusion_err!("Length out of bounds"))?;
        let offsets = (0..len).collect();

        Self::new(type_ids, offsets)
    }

    /// Creates a new [TypedValueArrayBuilder] that will only have a single sub array with values
    /// or null.
    pub fn new_with_nullable_single_type(
        type_id: i8,
        null_buffer: &NullBuffer,
    ) -> DFResult<Self> {
        if null_buffer.null_count() == 0 {
            return Self::new_with_single_type(type_id, null_buffer.len());
        }

        let type_ids = null_buffer
            .iter()
            .map(|is_null| {
                if is_null {
                    type_id
                } else {
                    TypedValueEncodingField::Null.type_id()
                }
            })
            .collect();

        let mut offsets = Vec::with_capacity(null_buffer.len());
        let mut nulls = 0;
        let mut non_nulls = 0;
        for is_null in null_buffer.iter() {
            if is_null {
                offsets.push(nulls);
                nulls += 1;
            } else {
                offsets.push(non_nulls);
                non_nulls += 1;
            }
        }

        let result = Self::new(type_ids, offsets)?
            .with_nulls(Arc::new(NullArray::new(null_buffer.null_count())));
        Ok(result)
    }

    /// Sets the null array.
    pub fn with_nulls(mut self, array: Arc<dyn Array>) -> Self {
        self.nulls = Some(array);
        self
    }

    /// Sets the named nodes array.
    pub fn with_named_nodes(mut self, array: Arc<dyn Array>) -> Self {
        self.named_nodes = Some(array);
        self
    }

    /// Sets the blank nodes array.
    pub fn with_blank_nodes(mut self, array: Arc<dyn Array>) -> Self {
        self.blank_nodes = Some(array);
        self
    }

    /// Sets the string array
    pub fn with_strings(mut self, array: Arc<dyn Array>) -> Self {
        self.strings = Some(array);
        self
    }

    /// Sets the boolean array.
    pub fn with_booleans(mut self, array: Arc<dyn Array>) -> Self {
        self.booleans = Some(array);
        self
    }

    /// Sets the float array.
    pub fn with_floats(mut self, array: Arc<dyn Array>) -> Self {
        self.floats = Some(array);
        self
    }

    /// Sets the double array.
    pub fn with_doubles(mut self, array: Arc<dyn Array>) -> Self {
        self.doubles = Some(array);
        self
    }

    /// Sets the decimal array.
    pub fn with_decimals(mut self, array: Arc<dyn Array>) -> Self {
        self.decimals = Some(array);
        self
    }

    /// Sets the int32 array.
    pub fn with_ints(mut self, array: Arc<dyn Array>) -> Self {
        self.ints = Some(array);
        self
    }

    /// Sets the integer array.
    pub fn with_integers(mut self, array: Arc<dyn Array>) -> Self {
        self.integers = Some(array);
        self
    }

    /// Sets the date times array.
    pub fn with_date_times(mut self, array: Arc<dyn Array>) -> Self {
        self.date_times = Some(array);
        self
    }

    /// Sets the times array.
    pub fn with_times(mut self, array: Arc<dyn Array>) -> Self {
        self.times = Some(array);
        self
    }

    /// Sets the dates array.
    pub fn with_dates(mut self, array: Arc<dyn Array>) -> Self {
        self.dates = Some(array);
        self
    }

    /// Sets the durations array.
    pub fn with_durations(mut self, array: Arc<dyn Array>) -> Self {
        self.durations = Some(array);
        self
    }

    /// Sets the typed literals array.
    pub fn with_typed_literals(mut self, array: Arc<dyn Array>) -> Self {
        self.typed_literals = Some(array);
        self
    }

    /// Tries to create a new [TypedValueArray] and validates the given arrays.
    ///
    /// For a list of invariants that must be upheld, see [UnionArray::try_new].
    pub fn finish(self) -> DFResult<TypedValueArray> {
        TYPED_VALUE_ENCODING.try_new_array(Arc::new(UnionArray::try_new(
            TypedValueEncoding::fields(),
            ScalarBuffer::from(self.type_ids),
            Some(ScalarBuffer::from(self.offsets)),
            vec![
                self.nulls.unwrap_or_else(|| Arc::new(NullArray::new(0))),
                self.named_nodes
                    .unwrap_or_else(|| Arc::new(StringArray::new_null(0))),
                self.blank_nodes
                    .unwrap_or_else(|| Arc::new(StringArray::new_null(0))),
                self.strings.unwrap_or_else(|| {
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::string_fields(),
                        0,
                    ))
                }),
                self.booleans
                    .unwrap_or_else(|| Arc::new(BooleanArray::new_null(0))),
                self.floats
                    .unwrap_or_else(|| Arc::new(Float32Array::new_null(0))),
                self.doubles
                    .unwrap_or_else(|| Arc::new(Float64Array::new_null(0))),
                self.decimals
                    .unwrap_or_else(|| Arc::new(Decimal128Array::new_null(0))),
                self.ints
                    .unwrap_or_else(|| Arc::new(Int32Array::new_null(0))),
                self.integers
                    .unwrap_or_else(|| Arc::new(Int64Array::new_null(0))),
                self.date_times.unwrap_or_else(|| {
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::timestamp_fields(),
                        0,
                    ))
                }),
                self.times.unwrap_or_else(|| {
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::timestamp_fields(),
                        0,
                    ))
                }),
                self.dates.unwrap_or_else(|| {
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::timestamp_fields(),
                        0,
                    ))
                }),
                self.durations.unwrap_or_else(|| {
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::duration_fields(),
                        0,
                    ))
                }),
                self.typed_literals.unwrap_or_else(|| {
                    Arc::new(StructArray::new_null(
                        TypedValueEncoding::typed_literal_fields(),
                        0,
                    ))
                }),
            ],
        )?))
    }
}
