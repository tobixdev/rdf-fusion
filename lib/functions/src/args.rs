use datafusion::common::plan_err;
use rdf_fusion_common::DFResult;
use rdf_fusion_model::vocab::xsd;
use rdf_fusion_model::{Iri, NamedNode, Term};
use std::collections::HashMap;

/// A builder for creating `RdfFusionFunctionArgs`.
pub struct RdfFusionFunctionArgsBuilder {
    /// The arguments as a map from name to term.
    values: HashMap<String, Term>,
}

impl RdfFusionFunctionArgsBuilder {
    /// Creates a new, empty builder.
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    /// Adds an argument to the builder.
    #[must_use]
    pub fn with_arg<TArg: RdfFusionFunctionArg>(mut self, name: String, value: TArg) -> Self {
        self.values.insert(name, value.into_term());
        self
    }

    /// Adds an optional argument to the builder.
    ///
    /// If the value is `None`, the argument is not added.
    #[must_use]
    pub fn with_optional_arg<TArg: RdfFusionFunctionArg>(
        mut self,
        name: String,
        value: Option<TArg>,
    ) -> Self {
        if let Some(value) = value {
            self.values.insert(name, value.into_term());
        }
        self
    }

    /// Builds the `RdfFusionFunctionArgs`.
    pub fn build(self) -> RdfFusionFunctionArgs {
        RdfFusionFunctionArgs::new(self.values)
    }
}

impl Default for RdfFusionFunctionArgsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A collection of arguments for an RDF Fusion function.
///
/// This is used to pass arguments to user-defined functions that are not part of the
/// standard SPARQL function signature. For example, the `IRI` function requires a base IRI for
/// resolving relative IRIs.
pub struct RdfFusionFunctionArgs {
    /// The arguments as a map from name to term.
    values: HashMap<String, Term>,
}

impl RdfFusionFunctionArgs {
    /// Creates a new [RdfFusionFunctionArgs] from a map of values.
    pub fn new(values: HashMap<String, Term>) -> Self {
        Self { values }
    }

    /// Creates an empty set of arguments.
    pub fn empty() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    /// Gets an argument by name.
    ///
    /// Returns `Ok(None)` if the argument is not found. Returns an error if the
    /// argument cannot be converted to the requested type.
    pub fn get<TArg: RdfFusionFunctionArg>(&self, name: &str) -> DFResult<Option<TArg>> {
        self.values
            .get(name)
            .map(|t| TArg::from_term(t.clone()))
            .transpose()
    }
}

/// Defines the names of built-in arguments that can be passed to RDF Fusion functions.
pub enum RdfFusionBuiltinArgNames {}

impl RdfFusionBuiltinArgNames {
    /// Name of the base IRI argument.
    pub const BASE_IRI: &'static str = "base_iri";

    /// Name of the base separator argument.
    pub const SEPARATOR: &'static str = "separator";
}

/// A trait for types that can be used as arguments to RDF Fusion functions.
///
/// This allows converting between native Rust types and RDF terms.
pub trait RdfFusionFunctionArg {
    /// Converts the argument into an RDF term.
    fn into_term(self) -> Term;

    /// Converts an RDF term into the argument type.
    fn from_term(term: Term) -> DFResult<Self>
    where
        Self: Sized;
}

impl RdfFusionFunctionArg for Iri<String> {
    fn into_term(self) -> Term {
        Term::NamedNode(NamedNode::new_unchecked(self.as_str().to_owned()))
    }

    fn from_term(term: Term) -> DFResult<Self> {
        if let Term::NamedNode(iri) = term {
            Ok(Iri::parse_unchecked(iri.as_str().to_owned()))
        } else {
            plan_err!("Argument must be a NamedNode.")
        }
    }
}

impl RdfFusionFunctionArg for String {
    fn into_term(self) -> Term {
        Term::Literal(self.into())
    }

    fn from_term(term: Term) -> DFResult<Self> {
        match term {
            Term::Literal(literal) if literal.datatype() == xsd::STRING => {
                Ok(literal.value().to_owned())
            }
            _ => plan_err!("Argument must be a string."),
        }
    }
}
