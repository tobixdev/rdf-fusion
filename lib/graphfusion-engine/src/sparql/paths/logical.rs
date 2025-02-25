use crate::DFResult;
use arrow_rdf::encoded::EncTerm;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use spargebra::algebra::PropertyPathExpression;
use spargebra::term::{NamedNodePattern, TermPattern};
use std::cmp::Ordering;
use std::fmt;

#[derive(PartialEq, Eq, Hash)]
pub struct PathNode {
    graph: Option<NamedNodePattern>,
    subject: TermPattern,
    path: PropertyPathExpression,
    object: TermPattern,
    schema: DFSchemaRef,
}

impl PathNode {
    pub fn new(
        graph: Option<NamedNodePattern>,
        subject: TermPattern,
        path: PropertyPathExpression,
        object: TermPattern,
    ) -> DFResult<Self> {
        let schema = compute_schema(graph.as_ref(), &subject, &path, &object);
        Ok(Self {
            graph,
            subject,
            path,
            object,
            schema,
        })
    }

    pub fn graph(&self) -> &Option<NamedNodePattern> {
        &self.graph
    }

    pub fn subject(&self) -> &TermPattern {
        &self.subject
    }

    pub fn path(&self) -> &PropertyPathExpression {
        &self.path
    }

    pub fn object(&self) -> &TermPattern {
        &self.object
    }
}

impl fmt::Debug for PathNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for PathNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for PathNode {
    fn name(&self) -> &str {
        "Path"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        Vec::new()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Path")
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        assert_eq!(inputs.len(), 0, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Ok(Self::new(
            self.graph.clone(),
            self.subject.clone(),
            self.path.clone(),
            self.object.clone(),
        )?)
    }
}

fn compute_schema(
    graph: Option<&NamedNodePattern>,
    subject: &TermPattern,
    path: &PropertyPathExpression,
    object: &TermPattern,
) -> DFSchemaRef {
    let mut vars = Vec::new();
    match graph {
        None => {}
        Some(graph) => vars.extend(get_variables_named_node(graph)),
    }
    vars.extend(get_variables_term(subject));
    vars.extend(get_variables_path(path));
    vars.extend(get_variables_term(object));

    let fields: Vec<_> = vars
        .iter()
        .map(|v| Field::new(v.to_string(), EncTerm::term_type(), true))
        .collect();
    DFSchemaRef::new(DFSchema::try_from(Schema::new(fields)).expect("Schema should be valid"))
}

fn get_variables_named_node(pattern: &NamedNodePattern) -> Vec<&str> {
    match pattern {
        NamedNodePattern::NamedNode(_) => Vec::new(),
        NamedNodePattern::Variable(var) => vec![var.as_str()],
    }
}

fn get_variables_term(pattern: &TermPattern) -> Vec<&str> {
    match pattern {
        TermPattern::Triple(triple) => {
            let mut result = Vec::new();
            result.extend(get_variables_term(&triple.subject));
            result.extend(get_variables_named_node(&triple.predicate));
            result.extend(get_variables_term(&triple.object));
            result
        }
        TermPattern::Variable(var) => vec![var.as_str()],
        _ => Vec::new(),
    }
}

fn get_variables_path(pattern: &PropertyPathExpression) -> Vec<&str> {
    match pattern {
        PropertyPathExpression::Reverse(inner) => get_variables_path(inner),
        PropertyPathExpression::Sequence(lhs, rhs) => {
            let mut variables = get_variables_path(lhs);
            variables.extend(get_variables_path(rhs));
            variables
        }
        PropertyPathExpression::Alternative(lhs, rhs) => {
            let mut variables = get_variables_path(lhs);
            variables.extend(get_variables_path(rhs));
            variables
        }
        PropertyPathExpression::ZeroOrMore(inner) => get_variables_path(inner),
        PropertyPathExpression::OneOrMore(inner) => get_variables_path(inner),
        PropertyPathExpression::ZeroOrOne(inner) => get_variables_path(inner),
        PropertyPathExpression::NegatedPropertySet(_) | PropertyPathExpression::NamedNode(_) => {
            Vec::new()
        }
    }
}
