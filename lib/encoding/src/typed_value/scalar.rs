use crate::TermEncoding;
use crate::encoding::EncodingScalar;
use crate::plain_term::PlainTermType;
use crate::typed_value::{
    TYPED_VALUE_ENCODING, TypedValueEncoding, TypedValueEncodingField,
};
use datafusion::common::{DataFusionError, ScalarValue, exec_err};
use rdf_fusion_common::DFResult;

/// Represents an Arrow scalar with a [TypedValueEncoding].
pub struct TypedValueScalar {
    /// The actual [ScalarValue].
    inner: ScalarValue,
}

impl TypedValueScalar {
    /// Tries to create a new [TypedValueScalar] from a regular [ScalarValue].
    ///
    /// # Errors
    ///
    /// Returns an error if the data type of `value` is unexpected.
    pub fn try_new(value: ScalarValue) -> DFResult<Self> {
        if value.data_type() != TYPED_VALUE_ENCODING.data_type() {
            return exec_err!(
                "Expected scalar value with TypedValueEncoding, got {:?}",
                value
            );
        }
        Ok(Self::new_unchecked(value))
    }

    /// Creates a new [TypedValueScalar] without checking invariants.
    pub fn new_unchecked(inner: ScalarValue) -> Self {
        Self { inner }
    }

    /// Returns the [PlainTermType] of this scalar.
    ///
    /// Returns [None] if this scalar is null.
    pub fn term_type(&self) -> Option<PlainTermType> {
        let ScalarValue::Union(Some((type_id, _)), _, _) = &self.inner else {
            panic!("Expected Union scalar value");
        };

        let type_id =
            TypedValueEncodingField::try_from(*type_id).expect("Expected valid type ID");
        match type_id {
            TypedValueEncodingField::Null => None,
            TypedValueEncodingField::NamedNode => Some(PlainTermType::NamedNode),
            TypedValueEncodingField::BlankNode => Some(PlainTermType::BlankNode),
            _ => Some(PlainTermType::Literal),
        }
    }
}

impl TryFrom<ScalarValue> for TypedValueScalar {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl EncodingScalar for TypedValueScalar {
    type Encoding = TypedValueEncoding;

    fn encoding(&self) -> &Self::Encoding {
        &TYPED_VALUE_ENCODING
    }

    fn scalar_value(&self) -> &ScalarValue {
        &self.inner
    }

    fn into_scalar_value(self) -> ScalarValue {
        self.inner
    }
}
