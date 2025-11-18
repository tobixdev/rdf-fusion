use rdf_fusion_encoding::{EncodingDatum, RdfFusionEncodings, TermEncoding};
use std::sync::Arc;

/// The arguments of invoking a [ScalarSparqlOp](crate::scalar::ScalarSparqlOp).
pub struct ScalarSparqlOpArgs<TEncoding: TermEncoding> {
    /// A reference to the encoding of the arguments.
    pub encoding: Arc<TEncoding>,
    /// A reference to the encodings.
    pub encodings: RdfFusionEncodings,
    /// The number of rows.
    ///
    /// This is important for nullary operations and scalar arguments.
    pub number_rows: usize,
    /// The arguments in a given encoding.
    pub args: Vec<EncodingDatum<TEncoding>>,
}
