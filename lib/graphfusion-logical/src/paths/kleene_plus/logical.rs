use crate::paths::PATH_TABLE_DFSCHEMA;
use crate::DFResult;
use datafusion::common::{plan_err, DFSchemaRef};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use std::cmp::Ordering;
use std::fmt;

#[derive(PartialEq, Eq, Hash)]
pub struct KleenePlusPathNode {
    inner: LogicalPlan,
    schema: DFSchemaRef,
}

impl KleenePlusPathNode {
    pub fn try_new(inner: LogicalPlan) -> DFResult<Self> {
        if inner.schema().as_ref() != PATH_TABLE_DFSCHEMA.as_ref() {
            return plan_err!(
                "Unexpected schema for inner path node. Schema: {}",
                inner.schema()
            );
        }

        Ok(Self {
            inner,
            schema: PATH_TABLE_DFSCHEMA.clone(),
        })
    }

    pub fn inner(&self) -> &LogicalPlan {
        &self.inner
    }
}

impl fmt::Debug for KleenePlusPathNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for KleenePlusPathNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for KleenePlusPathNode {
    fn name(&self) -> &str {
        "KleenePlusPath"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.inner()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "KleenePlusPath:",)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        assert_eq!(inputs.len(), 1, "input size inconsistent");
        assert_eq!(exprs.len(), 0, "expression size inconsistent");
        Ok(Self::try_new(inputs.pop().unwrap())?)
    }
}
