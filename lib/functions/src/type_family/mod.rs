//! Contains the infrastructure for dispatching functions on a [`TypedValueEncoding`] array.

mod error;
mod result;
mod scalar_op;
mod scalar_op_builder;

pub use error::*;
pub use result::*;
pub use scalar_op::*;
pub use scalar_op_builder::*;
