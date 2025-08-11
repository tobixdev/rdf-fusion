use crate::memory::object_id::{DEFAULT_GRAPH_ID, EncodedGraphObjectId, EncodedObjectId};
use datafusion::arrow::array::{Array, StructArray, UInt32Array};

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct EncodedQuad {
    pub graph_name: EncodedGraphObjectId,
    pub subject: EncodedObjectId,
    pub predicate: EncodedObjectId,
    pub object: EncodedObjectId,
}

pub struct EncodedQuadArray<'array> {
    array: &'array StructArray,
}

pub struct EncodedQuadArrayParts<'array> {
    pub graph_name: &'array UInt32Array,
    pub subject: &'array UInt32Array,
    pub predicate: &'array UInt32Array,
    pub object: &'array UInt32Array,
}

impl<'array> EncodedQuadArray<'array> {
    /// Creates a new [EncodedQuadArray].
    pub fn new(array: &'array StructArray) -> Self {
        Self { array }
    }

    /// Returns the length of the array.
    pub fn len(&self) -> usize {
        self.array.len()
    }

    /// Returns typed references to the parts of this array.
    pub fn as_parts(&self) -> EncodedQuadArrayParts<'_> {
        let graph_name = self
            .array
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("Schema known");
        let subject = self
            .array
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("Schema known");
        let predicate = self
            .array
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("Schema known");
        let object = self
            .array
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("Schema known");
        EncodedQuadArrayParts {
            graph_name,
            subject,
            predicate,
            object,
        }
    }
}

impl<'array> IntoIterator for &'array EncodedQuadArray<'array> {
    type Item = EncodedQuad;
    type IntoIter = EncodedQuadIterator<'array>;

    fn into_iter(self) -> Self::IntoIter {
        EncodedQuadIterator {
            array: self.as_parts(),
            next_index: 0,
        }
    }
}

/// A simple iterator over a [`EncodedQuadArray`]
pub struct EncodedQuadIterator<'array> {
    /// The array to iterate over
    array: EncodedQuadArrayParts<'array>,
    /// The next index to return
    next_index: usize,
}

impl Iterator for EncodedQuadIterator<'_> {
    type Item = EncodedQuad;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_index >= self.array.graph_name.len() {
            return None;
        }

        let result = EncodedQuad {
            graph_name: self
                .array
                .graph_name
                .is_valid(self.next_index)
                .then(|| self.array.graph_name.value(self.next_index))
                .map(EncodedObjectId::from)
                .map(EncodedGraphObjectId::from)
                .unwrap_or(DEFAULT_GRAPH_ID),
            subject: self.array.subject.values()[self.next_index].into(),
            predicate: self.array.predicate.values()[self.next_index].into(),
            object: self.array.object.values()[self.next_index].into(),
        };

        self.next_index += 1;
        Some(result)
    }
}
