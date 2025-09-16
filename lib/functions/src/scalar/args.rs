use rdf_fusion_encoding::{EncodingDatum, TermEncoding};

pub struct ScalarSparqlOpArgs<TEncoding: TermEncoding> {
    pub number_rows: usize,
    pub args: Vec<EncodingDatum<TEncoding>>,
}
