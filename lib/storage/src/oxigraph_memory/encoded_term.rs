use graphfusion_model::{
    BlankNode, GraphNameRef, Literal, NamedNode, NamedNodeRef, NamedOrBlankNodeRef, SubjectRef,
    TermRef,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EncodedTerm {
    DefaultGraph,
    NamedNode(NamedNode),
    BlankNode(BlankNode),
    Literal(Literal),
}

impl From<NamedOrBlankNodeRef<'_>> for EncodedTerm {
    fn from(value: NamedOrBlankNodeRef<'_>) -> Self {
        match value {
            NamedOrBlankNodeRef::NamedNode(node) => Self::NamedNode(node.into_owned()),
            NamedOrBlankNodeRef::BlankNode(node) => Self::BlankNode(node.into_owned()),
        }
    }
}

impl From<GraphNameRef<'_>> for EncodedTerm {
    fn from(value: GraphNameRef<'_>) -> Self {
        match value {
            GraphNameRef::DefaultGraph => Self::DefaultGraph,
            GraphNameRef::NamedNode(node) => Self::NamedNode(node.into_owned()),
            GraphNameRef::BlankNode(node) => Self::BlankNode(node.into_owned()),
        }
    }
}

impl From<SubjectRef<'_>> for EncodedTerm {
    fn from(value: SubjectRef<'_>) -> Self {
        match value {
            SubjectRef::NamedNode(node) => Self::NamedNode(node.into_owned()),
            SubjectRef::BlankNode(node) => Self::BlankNode(node.into_owned()),
        }
    }
}

impl From<NamedNodeRef<'_>> for EncodedTerm {
    fn from(value: NamedNodeRef<'_>) -> Self {
        Self::NamedNode(value.into_owned())
    }
}

impl From<TermRef<'_>> for EncodedTerm {
    fn from(value: TermRef<'_>) -> Self {
        match value {
            TermRef::NamedNode(node) => Self::NamedNode(node.into_owned()),
            TermRef::BlankNode(node) => Self::BlankNode(node.into_owned()),
            TermRef::Literal(node) => Self::Literal(node.into_owned()),
        }
    }
}

impl EncodedTerm {
    pub fn is_default_graph(&self) -> bool {
        matches!(self, Self::DefaultGraph)
    }
}
