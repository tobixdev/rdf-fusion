use crate::expr_builder::GraphFusionExprBuilder;
use crate::DFResult;
use datafusion::common::Column;
use datafusion::logical_expr::Expr;
use datafusion::prelude::col;
use graphfusion_encoding::TermEncoding;
use graphfusion_model::{Literal, NamedNode};
use spargebra::term::{BlankNode, GraphNamePattern, NamedNodePattern, Term, TermPattern, Variable};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// An element that can be part of a [PatternNode]. This enum is the union of all pattern variants.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub enum PatternNodeElement {
    /// A named node pattern.
    NamedNode(NamedNode),
    /// A blank node pattern.
    BlankNode(BlankNode),
    /// A literal pattern.
    Literal(Literal),
    /// A variable pattern.
    Variable(Variable),
    /// A default graph pattern.
    DefaultGraph,
    /// No pattern. This will lead to no filter and no variable.
    #[default]
    None,
}

impl PatternNodeElement {
    /// Creates an [Expr] that filters `column` based on the contents of this element.
    #[allow(clippy::unwrap_in_result, reason = "TODO")]
    pub fn filter_expression(
        &self,
        factory: &GraphFusionExprBuilder,
        column: &Column,
    ) -> DFResult<Option<Expr>> {
        let result = match self {
            PatternNodeElement::NamedNode(nn) => Some(
                factory.filter_by_scalar(col(column.clone()), Term::from(nn.clone()).as_ref())?,
            ),
            PatternNodeElement::Literal(lit) => Some(
                factory.filter_by_scalar(col(column.clone()), Term::from(lit.clone()).as_ref())?,
            ),
            PatternNodeElement::BlankNode(_) => {
                // A blank node indicates that this should be a non-default graph.
                Some(Expr::from(column.clone()).is_not_null())
            }
            PatternNodeElement::DefaultGraph => Some(Expr::from(column.clone()).is_null()),
            _ => None,
        };
        Ok(result)
    }

    /// Returns a reference to a possible variable.
    pub fn variable_name(&self) -> Option<String> {
        match self {
            PatternNodeElement::BlankNode(bnode) => Some(format!("_:{}", bnode.as_ref().as_str())),
            PatternNodeElement::Variable(var) => Some(var.as_str().into()),
            _ => None,
        }
    }
}

impl Display for PatternNodeElement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            PatternNodeElement::NamedNode(nn) => nn.fmt(f),
            PatternNodeElement::BlankNode(bnode) => bnode.fmt(f),
            PatternNodeElement::Literal(lit) => lit.fmt(f),
            PatternNodeElement::Variable(var) => var.fmt(f),
            PatternNodeElement::DefaultGraph => f.write_str("DefaultGraph"),
            PatternNodeElement::None => f.write_str("None"),
        }
    }
}

impl From<TermPattern> for PatternNodeElement {
    fn from(pattern: TermPattern) -> Self {
        match pattern {
            TermPattern::NamedNode(nn) => PatternNodeElement::NamedNode(nn),
            TermPattern::BlankNode(bnode) => PatternNodeElement::BlankNode(bnode),
            TermPattern::Literal(lit) => PatternNodeElement::Literal(lit),
            TermPattern::Variable(var) => PatternNodeElement::Variable(var),
        }
    }
}

impl From<NamedNodePattern> for PatternNodeElement {
    fn from(value: NamedNodePattern) -> Self {
        match value {
            NamedNodePattern::NamedNode(nn) => PatternNodeElement::NamedNode(nn),
            NamedNodePattern::Variable(var) => PatternNodeElement::Variable(var),
        }
    }
}

impl From<GraphNamePattern> for PatternNodeElement {
    fn from(value: GraphNamePattern) -> Self {
        match value {
            GraphNamePattern::DefaultGraph => PatternNodeElement::DefaultGraph,
            GraphNamePattern::NamedNode(nn) => PatternNodeElement::NamedNode(nn),
            GraphNamePattern::Variable(var) => PatternNodeElement::Variable(var),
        }
    }
}
