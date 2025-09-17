/// Represents the name of a single [TermEncoding](crate::TermEncoding).
///
/// RDF Fusion allows users to define multiple encodings for RDF terms. This allows specializing the
/// Arrow arrays used for holding the results of queries.
///
/// # Order
///
/// The order defined over the [EncodingName] defines how much information they preserve.
/// - [Self::ObjectId] and [Self::PlainTerm] preserve the entire information.
/// - [Self::TypedValue] preserves the value of the term, but not their lexical form.
/// - [Self::Sortable] can loose information (e.g., precision in numerics)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EncodingName {
    /// Name of the [ObjectIdEncoding](crate::object_id::ObjectIdEncoding). Represents all terms,
    /// including literals, as a unique identifier.
    ObjectId,
    /// Name of the [PlainTermEncoding](crate::plain_term::PlainTermEncoding). Represents all terms,
    /// including literals, using their lexical value.
    PlainTerm,
    /// Name of the [TypedValueEncoding](crate::typed_value::TypedValueEncoding). Represents
    /// IRIs and blank nodes using their lexical value and literals as their typed value.
    TypedValue,
    /// Name of the [SortableTermEncoding](crate::sortable_term::SortableTermEncoding) which is used
    /// for sorting. We plan to remove this encoding in the future, once we can introduce custom
    /// orderings into the query engine.
    Sortable,
}
