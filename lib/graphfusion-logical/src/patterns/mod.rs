mod logical;
mod rewrite;

pub use logical::PatternNode;
pub use rewrite::PatternToProjectionRule;
use spargebra::term::TermPattern;

fn pattern_to_variable_name(pattern: &TermPattern) -> Option<String> {
    match pattern {
        TermPattern::BlankNode(bnode) => Some(format!("_:{}", bnode.as_ref().as_str())),
        TermPattern::Variable(var) => Some(var.as_str().into()),
        _ => None,
    }
}
