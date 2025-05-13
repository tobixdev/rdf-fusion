use crate::scalar_encoder::ScalarEncoder;
use crate::typed_value::scalar::TermValueScalar;
use crate::typed_value::{TypedValueArrayBuilder, TypedValueEncoding, TypedValueEncodingField};
use datafusion::arrow::datatypes::UnionMode;
use datafusion::common::ScalarValue;
use rdf_fusion_model::{BlankNodeRef, GraphNameRef, LiteralRef, NamedNodeRef};

pub struct TermValueScalarEncoder {}

impl ScalarEncoder for TermValueScalarEncoder {
    type Scalar = TermValueScalar;

    fn encode_scalar_graph(graph: GraphNameRef<'_>) -> Self::Scalar {
        match graph {
            GraphNameRef::NamedNode(nn) => Self::encode_scalar_named_node(nn),
            GraphNameRef::BlankNode(bnode) => Self::encode_scalar_blank_node(bnode),
            GraphNameRef::DefaultGraph => Self::encode_scalar_null(),
        }
    }

    fn encode_scalar_null() -> Self::Scalar {
        let value = ScalarValue::Union(
            Some((
                TypedValueEncodingField::Null.type_id(),
                Box::new(ScalarValue::Null),
            )),
            TypedValueEncoding::fields(),
            UnionMode::Dense,
        );
        Self::Scalar::new_unchecked(value)
    }

    fn encode_scalar_named_node(node: NamedNodeRef<'_>) -> Self::Scalar {
        let string_value = ScalarValue::Utf8(Some(String::from(node.as_str())));
        let value = ScalarValue::Union(
            Some((
                TypedValueEncodingField::NamedNode.type_id(),
                Box::new(string_value),
            )),
            TypedValueEncoding::fields(),
            UnionMode::Dense,
        );
        Self::Scalar::new_unchecked(value)
    }

    fn encode_scalar_blank_node(node: BlankNodeRef<'_>) -> Self::Scalar {
        let string_value = ScalarValue::Utf8(Some(String::from(node.as_str())));
        let value = ScalarValue::Union(
            Some((
                TypedValueEncodingField::BlankNode.type_id(),
                Box::new(string_value),
            )),
            TypedValueEncoding::fields(),
            UnionMode::Dense,
        );
        Self::Scalar::new_unchecked(value)
    }

    fn encode_scalar_literal(literal: LiteralRef<'_>) -> Self::Scalar {
        let mut builder = TypedValueArrayBuilder::default();
        builder
            .append_literal(literal)
            .expect("Cannot become too long");
        let array = builder.finish();
        let scalar =
            ScalarValue::try_from_array(&array, 0).expect("Only supported Scalar are used");
        Self::Scalar::new_unchecked(scalar)
    }
}
