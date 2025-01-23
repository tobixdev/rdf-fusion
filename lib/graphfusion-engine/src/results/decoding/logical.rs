use crate::results::decoding::compute_decoded_schema;
use crate::DFResult;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use std::cmp::Ordering;
use std::fmt;

#[derive(PartialEq, Eq, Hash)]
pub struct DecodeRdfTermsNode {
    input: LogicalPlan,
    schema: DFSchemaRef,
}

impl DecodeRdfTermsNode {
    pub fn new(input: LogicalPlan) -> DFResult<Self> {
        let new_schema = compute_decoded_schema(input.schema().as_arrow());
        Ok(Self {
            input,
            schema: DFSchemaRef::new(DFSchema::try_from(new_schema)?),
        })
    }
}

impl fmt::Debug for DecodeRdfTermsNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for DecodeRdfTermsNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.input.partial_cmp(&other.input) // self.schema is a function of self.input
    }
}

impl UserDefinedLogicalNodeCore for DecodeRdfTermsNode {
    fn name(&self) -> &str {
        "RdfTermDecoder"
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
        write!(f, "DecodeRdfTerms")
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Ok(Self::new(inputs.pop().unwrap())?)
    }
}
