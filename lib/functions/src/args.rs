use datafusion::common::plan_err;
use rdf_fusion_common::DFResult;
use rdf_fusion_model::vocab::xsd;
use rdf_fusion_model::{Iri, NamedNode, Term};
use std::collections::HashMap;

pub struct RdfFusionFunctionArgsBuilder {
    /// TODO
    values: HashMap<String, Term>,
}

impl RdfFusionFunctionArgsBuilder {
    /// TODO
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    /// TODO
    #[must_use]
    pub fn with_arg<TArg: RdfFusionFunctionArg>(mut self, name: String, value: TArg) -> Self {
        self.values.insert(name, value.into_term());
        self
    }

    /// TODO
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

    /// TODO
    pub fn build(self) -> RdfFusionFunctionArgs {
        RdfFusionFunctionArgs::new(self.values)
    }
}

impl Default for RdfFusionFunctionArgsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// TODO
pub struct RdfFusionFunctionArgs {
    /// TODO
    values: HashMap<String, Term>,
}

impl RdfFusionFunctionArgs {
    /// TODO
    pub fn new(values: HashMap<String, Term>) -> Self {
        Self { values }
    }

    /// TODO
    pub fn empty() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    /// TODO
    pub fn get<TArg: RdfFusionFunctionArg>(&self, name: &str) -> DFResult<Option<TArg>> {
        self.values
            .get(name)
            .map(|t| TArg::from_term(t.clone()))
            .transpose()
    }
}

/// TODO
pub enum RdfFusionBuiltinArgNames {}

impl RdfFusionBuiltinArgNames {
    /// Name of the base IRI argument.
    pub const BASE_IRI: &'static str = "base_iri";

    /// Name of the base separator argument.
    pub const SEPARATOR: &'static str = "separator";
}

/// TODO
pub trait RdfFusionFunctionArg {
    /// TODO
    fn into_term(self) -> Term;

    /// TODO
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
