use crate::patterns::pattern_to_variable_name;
use crate::DFResult;
use arrow_rdf::encoded::EncTerm;
use datafusion::arrow::datatypes::{Field, Fields};
use datafusion::common::{plan_err, DFSchema, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use spargebra::term::TermPattern;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

#[derive(PartialEq, Eq, Hash)]
pub struct PatternNode {
    input: LogicalPlan,
    patterns: Vec<TermPattern>,
    schema: DFSchemaRef,
}

impl PatternNode {
    pub fn try_new(input: LogicalPlan, patterns: Vec<TermPattern>) -> DFResult<Self> {
        if input.schema().columns().len() != patterns.len() {
            return plan_err!("Patterns must match the number of column of inner.");
        }

        let schema = Self::compute_schema(&patterns);
        Ok(Self {
            input,
            patterns,
            schema,
        })
    }

    pub(crate) fn compute_schema(patterns: &Vec<TermPattern>) -> DFSchemaRef {
        let mut fields = Vec::new();
        for pattern in patterns {
            match pattern_to_variable_name(pattern) {
                None => {}
                Some(variable) => {
                    if !fields.contains(&variable) {
                        fields.push(variable);
                    }
                }
            }
        }

        let fields = fields
            .into_iter()
            .map(|name| Field::new(name, EncTerm::data_type(), true))
            .collect::<Fields>();
        Arc::new(DFSchema::from_unqualified_fields(fields, HashMap::new()).expect("Names correct"))
    }

    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    pub fn patterns(&self) -> &[TermPattern] {
        &self.patterns
    }
}

impl fmt::Debug for PatternNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for PatternNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for PatternNode {
    fn name(&self) -> &str {
        "Pattern"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let patterns = self
            .patterns
            .iter()
            .map(format_pattern)
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "Pattern: {patterns}",)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Self::try_new(inputs[0].clone(), self.patterns.clone())
    }
}

fn format_pattern(pattern: &TermPattern) -> String {
    match pattern {
        TermPattern::NamedNode(nn) => nn.to_string(),
        TermPattern::BlankNode(bnode) => bnode.to_string(),
        TermPattern::Literal(v) => v.value().to_owned(),
        TermPattern::Triple(_) => unreachable!(),
        TermPattern::Variable(v) => v.to_string(),
    }
}
