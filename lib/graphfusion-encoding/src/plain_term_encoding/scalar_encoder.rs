use crate::scalar_encoder::ScalarEncoder;
use model::{
    BlankNodeRef,
    GraphNameRef, LiteralRef, NamedNodeRef,
};
use crate::plain_term_encoding::PlainTermScalar;

pub struct PlainTermScalarEncoder {}

impl ScalarEncoder for PlainTermScalarEncoder {
    type Scalar = PlainTermScalar;

    fn encode_scalar_graph(graph: GraphNameRef<'_>) -> Self::Scalar {
        Self::encode_scalar_null()
    }

    fn encode_scalar_null() -> Self::Scalar {
        todo!()
    }

    fn encode_scalar_named_node(node: NamedNodeRef<'_>) -> Self::Scalar {
        todo!()
    }

    fn encode_scalar_blank_node(node: BlankNodeRef<'_>) -> Self::Scalar {
        todo!()
    }

    fn encode_scalar_literal(literal: LiteralRef<'_>) -> Self::Scalar {
        todo!()
    }
}
