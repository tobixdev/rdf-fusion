use crate::encoding::EncodingScalar;
use crate::plain_term::decoders::{
    DefaultPlainTermDecoder, GraphNameRefPlainTermDecoder,
};
use crate::plain_term::encoders::DefaultPlainTermEncoder;
use crate::plain_term::{PLAIN_TERM_ENCODING, PlainTermEncoding};
use crate::{TermDecoder, TermEncoder};
use datafusion::common::{DataFusionError, ScalarValue, exec_err};
use rdf_fusion_common::DFResult;
use rdf_fusion_model::{
    BlankNodeRef, GraphNameRef, LiteralRef, NamedNodeRef, NamedOrBlankNodeRef,
    SubjectRef, Term, TermRef, ThinError, ThinResult,
};

/// Represents an Arrow scalar with a [PlainTermEncoding].
pub struct PlainTermScalar {
    inner: ScalarValue,
}

impl PlainTermScalar {
    /// Tries to create a new [PlainTermScalar] from a regular [ScalarValue].
    ///
    /// # Errors
    ///
    /// Returns an error if the data type of `value` is unexpected.
    pub fn try_new(value: ScalarValue) -> DFResult<Self> {
        if value.data_type() != PlainTermEncoding::data_type() {
            return exec_err!(
                "Expected scalar value with PlainTermEncoding, got {:?}",
                value
            );
        }
        Ok(Self::new_unchecked(value))
    }

    /// Creates a new [PlainTermScalar] from the given `graph_name`.
    pub fn from_graph_name(graph_name: GraphNameRef<'_>) -> DFResult<Self> {
        match graph_name {
            GraphNameRef::NamedNode(nn) => Ok(Self::from(nn)),
            GraphNameRef::BlankNode(bnode) => Ok(Self::from(bnode)),
            GraphNameRef::DefaultGraph => {
                DefaultPlainTermEncoder::encode_term(ThinError::expected())
            }
        }
    }

    /// Creates a new [PlainTermScalar] without checking invariants.
    pub fn new_unchecked(inner: ScalarValue) -> Self {
        Self { inner }
    }
}

impl EncodingScalar for PlainTermScalar {
    type Encoding = PlainTermEncoding;

    fn encoding(&self) -> &Self::Encoding {
        &PLAIN_TERM_ENCODING
    }

    fn scalar_value(&self) -> &ScalarValue {
        &self.inner
    }

    fn into_scalar_value(self) -> ScalarValue {
        self.inner
    }
}

impl TryFrom<ScalarValue> for PlainTermScalar {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl From<TermRef<'_>> for PlainTermScalar {
    fn from(term: TermRef<'_>) -> Self {
        DefaultPlainTermEncoder::encode_term(Ok(term)).expect("Always Ok given")
    }
}

impl From<SubjectRef<'_>> for PlainTermScalar {
    fn from(term: SubjectRef<'_>) -> Self {
        match term {
            SubjectRef::NamedNode(nn) => Self::from(nn),
            SubjectRef::BlankNode(bnode) => Self::from(bnode),
        }
    }
}

impl From<NamedNodeRef<'_>> for PlainTermScalar {
    fn from(term: NamedNodeRef<'_>) -> Self {
        Self::from(TermRef::NamedNode(term))
    }
}

impl From<BlankNodeRef<'_>> for PlainTermScalar {
    fn from(term: BlankNodeRef<'_>) -> Self {
        Self::from(TermRef::BlankNode(term))
    }
}

impl From<NamedOrBlankNodeRef<'_>> for PlainTermScalar {
    fn from(term: NamedOrBlankNodeRef<'_>) -> Self {
        Self::from(TermRef::from(term))
    }
}

impl From<LiteralRef<'_>> for PlainTermScalar {
    fn from(term: LiteralRef<'_>) -> Self {
        Self::from(TermRef::Literal(term))
    }
}

impl From<Term> for PlainTermScalar {
    fn from(term: Term) -> Self {
        Self::from(term.as_ref())
    }
}

impl<'a> From<&'a PlainTermScalar> for GraphNameRef<'a> {
    fn from(value: &'a PlainTermScalar) -> Self {
        GraphNameRefPlainTermDecoder::decode_term(value)
            .expect("GraphName is always some")
    }
}

impl<'a> From<&'a PlainTermScalar> for ThinResult<TermRef<'a>> {
    fn from(value: &'a PlainTermScalar) -> Self {
        DefaultPlainTermDecoder::decode_term(value)
    }
}
