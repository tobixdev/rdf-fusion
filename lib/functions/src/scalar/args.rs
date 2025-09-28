use rdf_fusion_encoding::{EncodingDatum, TermEncoding};

/// The arguments of invoking a [ScalarSparqlOp](crate::scalar::ScalarSparqlOp).
pub struct ScalarSparqlOpArgs<TEncoding: TermEncoding> {
    /// The number of rows.
    ///
    /// This is important for nullary operations and scalar arguments.
    pub number_rows: usize,
    /// The arguments in a given encoding.
    pub args: Vec<EncodingDatum<TEncoding>>,
}
