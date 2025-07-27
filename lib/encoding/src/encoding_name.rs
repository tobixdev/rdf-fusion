use crate::object_id::ObjectIdEncoding;
use crate::plain_term::PlainTermEncoding;
use datafusion::arrow::datatypes::DataType;

/// Represents the name of a single [TermEncoding].
///
/// RDF Fusion allows users to define multiple encodings for RDF terms. This allows specializing the
/// Arrow arrays used for holding the results of queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EncodingName {
    /// Name of the [PlainTermEncoding](crate::plain_term::PlainTermEncoding). Represents all terms,
    /// including literals, using their lexical value.
    PlainTerm,
    /// Name of the [TypedValueEncoding](crate::typed_value::TypedValueEncoding). Represents
    /// IRIs and blank nodes using their lexical value and literals as their typed value.
    TypedValue,
    /// Name of the [ObjectIdEncoding](crate::object_id::ObjectIdEncoding). Represents all terms,
    /// including literals, as a unique identifier.
    ObjectId,
    /// Name of the [SortableTermEncoding](crate::sortable_term::SortableTermEncoding) which is used
    /// for sorting. We plan to remove this encoding in the future, once we can introduce custom
    /// orderings into the query engine.
    Sortable,
}

/// Represents the name of a single [TermEncoding] that map to a fixed Arrow type. As a result, the
/// configuration of an encoding cannot change how it is represented in the query engine.
///
/// This is a subset of [EncodingName]. The advantage of this enum is that it can be constructed
/// without an instance of [RdfFusionEncodings].
///
/// The Sortable encoding is not included as this is a workaround that should be removed in the
/// future.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StaticDataTypeEncodingName {
    /// See [EncodingName::PlainTerm].
    PlainTerm,
    /// See [EncodingName::ObjectId].
    ObjectId,
}

impl StaticDataTypeEncodingName {
    /// Tries to create an instance of [StaticDataTypeEncodingName] from the `data_type`.
    pub fn try_from_data_type(
        data_type: &DataType,
    ) -> Option<StaticDataTypeEncodingName> {
        if data_type == &PlainTermEncoding::data_type() {
            return Some(StaticDataTypeEncodingName::PlainTerm);
        }

        if data_type == &ObjectIdEncoding::data_type() {
            return Some(StaticDataTypeEncodingName::ObjectId);
        }

        None
    }
}
