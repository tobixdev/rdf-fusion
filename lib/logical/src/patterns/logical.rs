use crate::patterns::pattern_element::PatternNodeElement;
use crate::DFResult;
use datafusion::arrow::datatypes::{Field, Fields};
use datafusion::common::{plan_err, DFSchema, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use graphfusion_encoding::plain_term::PlainTermEncoding;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use graphfusion_encoding::TermEncoding;

#[derive(PartialEq, Eq, Hash)]
pub struct PatternNode {
    input: LogicalPlan,
    patterns: Vec<PatternNodeElement>,
    schema: DFSchemaRef,
}

impl PatternNode {
    /// Creates a new [PatternNode].
    ///
    /// # Errors
    ///
    /// Returns an error if the length of the input schema does not match the length of the
    /// patterns.
    pub fn try_new(input: LogicalPlan, patterns: Vec<PatternNodeElement>) -> DFResult<Self> {
        if input.schema().columns().len() != patterns.len() {
            return plan_err!("Patterns must match the number of column of inner.");
        }

        let schema = PatternNode::compute_schema(&patterns)?;
        Ok(Self {
            input,
            patterns,
            schema,
        })
    }

    pub(crate) fn compute_schema(patterns: &[PatternNodeElement]) -> DFResult<DFSchemaRef> {
        let mut fields = Vec::new();
        for pattern in patterns {
            match pattern.variable_name() {
                None => {}
                Some(variable) => {
                    if !fields.contains(&variable) {
                        fields.push(variable);
                    }
                }
            }
        }

        // TODO: base this on the inner
        let fields = fields
            .into_iter()
            .map(|name| Field::new(name, PlainTermEncoding::data_type(), true))
            .collect::<Fields>();
        Ok(Arc::new(DFSchema::from_unqualified_fields(
            fields,
            HashMap::new(),
        )?))
    }

    pub fn input(&self) -> &LogicalPlan {
        &self.input
    }

    pub fn patterns(&self) -> &[PatternNodeElement] {
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
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "Pattern: {patterns}",)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        if inputs.len() != 1 {
            return plan_err!(
                "PatternNode must have exactly one input, got {}",
                inputs.len()
            );
        }

        if !exprs.is_empty() {
            return plan_err!("PatternNode must have no expressions");
        }

        Self::try_new(inputs[0].clone(), self.patterns.clone())
    }
}
