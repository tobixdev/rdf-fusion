mod blank_node_mode;
mod error;
mod object_id;
pub mod quads;
mod rdf;
mod typed_value;
pub mod vocab;
mod xsd;

pub use blank_node_mode::BlankNodeMatchingMode;
pub use error::*;
pub use object_id::ObjectId;
pub use rdf::*;
pub use typed_value::*;
pub use xsd::*;

// Re-export some oxrdf types.
pub use oxiri::Iri;
pub use oxrdf::{
    dataset, BlankNode, BlankNodeRef, Dataset, Graph, GraphName, GraphNameRef,
    IriParseError, Literal, LiteralRef, NamedNode, NamedNodeRef, NamedOrBlankNode,
    NamedOrBlankNodeRef, Quad, QuadRef, Subject, SubjectRef, Term, TermParseError, TermRef, Triple,
    TripleRef, Variable, VariableNameParseError, VariableRef,
};
pub use spargebra::algebra::PropertyPathExpression;
pub use spargebra::term::{GroundTerm, NamedNodePattern, TermPattern, TriplePattern};

use datafusion::arrow::error::ArrowError;

pub type AResult<T> = Result<T, ArrowError>;
pub type DFResult<T> = datafusion::error::Result<T>;
