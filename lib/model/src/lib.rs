mod error;
mod rdf;
mod value;
pub mod vocab;
mod xsd;

pub use error::*;
pub use rdf::*;
pub use value::*;
pub use xsd::*;

// Re-export some oxrdf types.
pub use oxiri::Iri;
pub use oxrdf::Term as DecodedTerm;
pub use oxrdf::TermRef as DecodedTermRef;
pub use oxrdf::{
    BlankNode, BlankNodeRef, Graph, GraphName, GraphNameRef, IriParseError, Literal, LiteralRef,
    NamedNode, NamedNodeRef, NamedOrBlankNode, NamedOrBlankNodeRef, Quad, QuadRef, Subject,
    SubjectRef, TermParseError, Triple, TripleRef, Variable, VariableNameParseError, VariableRef,
};
