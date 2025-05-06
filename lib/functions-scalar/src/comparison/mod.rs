mod generic;
mod same_term;

pub use generic::{
    EqSparqlOp, GreaterOrEqualSparqlOp, GreaterThanSparqlOp, LessOrEqualSparqlOp, LessThanSparqlOp,
};
pub use same_term::SameTermSparqlOp;
