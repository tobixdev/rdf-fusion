use crate::builtin::BuiltinName;
use graphfusion_model::Iri;
use std::fmt::{Display, Formatter};

/// Identifier for a function. Either it is a GraphFusion builtin (e.g., a SPARQL operation) or a
/// custom function.
#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub enum FunctionName {
    /// A GraphFusion builtin function.
    Builtin(BuiltinName),
    /// A custom function.
    Custom(Iri<String>),
}

impl Display for FunctionName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionName::Builtin(builtin) => builtin.fmt(f),
            FunctionName::Custom(name) => name.fmt(f),
        }
    }
}
