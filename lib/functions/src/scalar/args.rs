use datafusion::common::ScalarValue;
use rdf_fusion_encoding::{EncodingDatum, TermEncoding};

pub struct ScalarSparqlOpArgs<TEncoding: TermEncoding> {
    pub number_rows: usize,
    pub args: Vec<EncodingDatum<TEncoding>>,
    pub constant_args: Vec<ScalarValue>,
}
