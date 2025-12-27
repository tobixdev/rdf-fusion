use crate::typed_value::family::TypeFamily;
use crate::typed_value::{TypedValueArray, TypedValueEncoding, TypedValueEncodingRef};
use crate::TermEncoding;
use datafusion::arrow::array::{new_empty_array, Array, ArrayRef, NullArray, UnionArray};
use datafusion::arrow::buffer::{NullBuffer, ScalarBuffer};
use datafusion::common::{exec_datafusion_err, exec_err};
use rdf_fusion_model::DFResult;
use std::sync::Arc;

/// Allows creating a [TypedValueArray] from its array parts.
pub struct TypedValueArrayBuilder {
    encoding: TypedValueEncodingRef,
    type_ids: Vec<i8>,
    offsets: Vec<i32>,
    arrays: Vec<Option<ArrayRef>>,
}

impl TypedValueArrayBuilder {
    /// Creates a new [TypedValueArrayBuilder] with the given `type_ids` and `offsets`.
    ///
    /// Use the `with_` methods to assemble the rest of the array.
    ///
    /// # Errors
    ///
    /// Returns an error if the length of the given arrays does not match.
    pub fn new(
        encoding: TypedValueEncodingRef,
        type_ids: Vec<i8>,
        offsets: Vec<i32>,
    ) -> DFResult<Self> {
        if type_ids.len() != offsets.len() {
            return exec_err!(
                "Length of type_ids and offsets do not match: {} != {}",
                type_ids.len(),
                offsets.len()
            );
        }

        let arrays_size = encoding.num_type_families() + 1;
        Ok(Self {
            encoding,
            type_ids,
            offsets,
            arrays: vec![None; arrays_size],
        })
    }

    /// Creates a new [TypedValueArrayBuilder] that will only have a single sub array with values.
    pub fn new_with_single_type(
        encoding: TypedValueEncodingRef,
        type_id: i8,
        len: usize,
    ) -> DFResult<Self> {
        let type_ids = vec![type_id; len];

        let len = i32::try_from(len)
            .map_err(|_| exec_datafusion_err!("Length out of bounds"))?;
        let offsets = (0..len).collect();

        Self::new(encoding, type_ids, offsets)
    }

    /// Creates a new [TypedValueArrayBuilder] that will only have a single sub array with values
    /// or null.
    pub fn new_with_nullable_single_type(
        encoding: TypedValueEncodingRef,
        type_id: i8,
        null_buffer: &NullBuffer,
    ) -> DFResult<Self> {
        if null_buffer.null_count() == 0 {
            return Self::new_with_single_type(encoding, type_id, null_buffer.len());
        }

        let type_ids = null_buffer
            .iter()
            .map(|is_null| {
                if is_null {
                    type_id
                } else {
                    TypedValueEncoding::NULL_TYPE_ID
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

        let result = Self::new(encoding, type_ids, offsets)?
            .with_nulls(Arc::new(NullArray::new(null_buffer.null_count())));
        Ok(result)
    }

    /// Sets the null array.
    pub fn with_nulls(mut self, array: ArrayRef) -> Self {
        self.arrays[TypedValueEncoding::NULL_TYPE_ID as usize] = Some(array);
        self
    }

    /// Sets the named nodes array.
    pub fn with_array(
        mut self,
        type_family: &dyn TypeFamily,
        array: Option<ArrayRef>,
    ) -> DFResult<Self> {
        let (type_id, _) = self.encoding.find_type_family(type_family.id()).ok_or(
            exec_datafusion_err!(
                "Type family {} not found in encoding {}",
                type_family.id(),
                self.encoding.name()
            ),
        )?;

        if let Some(array) = &array {
            if type_family.data_type() != array.data_type() {
                return exec_err!(
                    "Type family {} has data type {} but array has data type {}",
                    type_family.id(),
                    type_family.data_type(),
                    array.data_type()
                );
            }
        }

        self.arrays[type_id as usize] = array;
        Ok(self)
    }

    /// Tries to create a new [TypedValueArray] and validates the given arrays.
    ///
    /// For a list of invariants that must be upheld, see [UnionArray::try_new].
    pub fn finish(self) -> DFResult<TypedValueArray> {
        let mut arrays = Vec::with_capacity(self.arrays.len());
        // Handle null array (index 0)
        arrays.push(
            self.arrays[0]
                .clone()
                .unwrap_or_else(|| Arc::new(NullArray::new(0))),
        );

        // Handle family arrays (index 1..)
        for (i, family) in self.encoding.type_families().iter().enumerate() {
            let arr = self.arrays[i + 1]
                .clone()
                .unwrap_or_else(|| new_empty_array(family.data_type()));
            arrays.push(arr);
        }

        self.encoding.try_new_array(Arc::new(UnionArray::try_new(
            self.encoding.data_type_fields(),
            ScalarBuffer::from(self.type_ids),
            Some(ScalarBuffer::from(self.offsets)),
            arrays,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typed_value::encoding::TypedValueEncoding;
    use crate::typed_value::family::BooleanFamily;
    use crate::EncodingArray;
    use datafusion::arrow::array::BooleanArray;
    use datafusion::arrow::util::pretty::pretty_format_columns;

    #[test]
    fn test_build_mixed_array() {
        let encoding = Arc::new(TypedValueEncoding::default());
        let boolean_family = BooleanFamily::new();

        // We want to build: [Null, true, false]
        // Row 0: Null (Type 0, Offset 0)
        // Row 1: True (Type 3, Offset 0)
        // Row 2: False (Type 3, Offset 1)

        let type_ids = vec![0, 3, 3];
        let offsets = vec![0, 0, 1];

        let builder = TypedValueArrayBuilder::new(encoding, type_ids, offsets)
            .unwrap()
            .with_nulls(Arc::new(NullArray::new(1)))
            .with_array(
                &boolean_family,
                Some(Arc::new(BooleanArray::from(vec![true, false]))),
            )
            .unwrap();

        let array = builder.finish().unwrap();

        insta::assert_snapshot!(
            pretty_format_columns("col", &[array.into_array_ref()]).unwrap(),
            @r"
        +----------------------------+
        | col                        |
        +----------------------------+
        | {null=}                    |
        | {rdf-fusion.boolean=true}  |
        | {rdf-fusion.boolean=false} |
        +----------------------------+
        "
        );
    }
}
