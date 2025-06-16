use datafusion::arrow::datatypes::{DataType, Fields};
use datafusion::common::{plan_err, DFSchema, DFSchemaRef};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::EncodingName;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// TODO
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum SparqlJoinType {
    /// TODO
    Inner,
    /// TODO
    Left,
}

impl Display for SparqlJoinType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            SparqlJoinType::Inner => write!(f, "Inner"),
            SparqlJoinType::Left => write!(f, "Left"),
        }
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct SparqlJoinNode {
    lhs: LogicalPlan,
    rhs: LogicalPlan,
    filter: Option<Expr>,
    join_type: SparqlJoinType,
    schema: DFSchemaRef,
}

impl SparqlJoinNode {
    /// TODO
    pub fn try_new(
        lhs: LogicalPlan,
        rhs: LogicalPlan,
        filter: Option<Expr>,
        join_type: SparqlJoinType,
    ) -> DFResult<Self> {
        validate_inputs(&lhs, &rhs)?;
        let schema = compute_schema(join_type, &lhs, &rhs)?;

        if let Some(filter) = &filter {
            let (data_type, _) = filter.data_type_and_nullable(&schema)?;
            if data_type != DataType::Boolean {
                return plan_err!("Filter must be a boolean expression.");
            }
        }

        Ok(Self {
            lhs,
            rhs,
            filter,
            join_type,
            schema,
        })
    }

    /// TODO
    pub fn lhs(&self) -> &LogicalPlan {
        &self.lhs
    }

    /// TODO
    pub fn rhs(&self) -> &LogicalPlan {
        &self.rhs
    }

    /// TODO
    pub fn filter(&self) -> Option<&Expr> {
        self.filter.as_ref()
    }

    /// TODO
    pub fn join_type(&self) -> SparqlJoinType {
        self.join_type
    }
}

impl fmt::Debug for SparqlJoinNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl PartialOrd for SparqlJoinNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for SparqlJoinNode {
    fn name(&self) -> &str {
        "SparqlJoin"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.lhs(), self.rhs()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        match &self.filter {
            None => vec![],
            Some(filter) => vec![filter.clone()],
        }
    }

    fn fmt_for_explain(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let filter = self
            .filter
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
        write!(f, "SparqlJoin: {} {}", self.join_type, &filter)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        if exprs.len() > 1 {
            return plan_err!("SparqlJoinNode must not have more than one expression.");
        }

        let input_len = inputs.len();
        let Ok([lhs, rhs]) = TryInto::<[LogicalPlan; 2]>::try_into(inputs) else {
            return plan_err!("SparqlJoinNode must have exactly two inputs, actual: {input_len}");
        };

        let filter = exprs.first().cloned();
        Self::try_new(lhs, rhs, filter, self.join_type)
    }
}

/// Validates whether the two inputs are valid.
///
/// The following invariants are checked:
/// - Join variables must have the PlainTermEncoding.
#[allow(clippy::expect_used)]
fn validate_inputs(lhs: &LogicalPlan, rhs: &LogicalPlan) -> DFResult<()> {
    let lhs_fields = lhs
        .schema()
        .fields()
        .iter()
        .map(|f| f.name())
        .collect::<HashSet<_>>();
    let rhs_fields = rhs
        .schema()
        .fields()
        .iter()
        .map(|f| f.name())
        .collect::<HashSet<_>>();

    for field_name in lhs_fields.intersection(&rhs_fields) {
        let lhs_field = lhs
            .schema()
            .field_with_unqualified_name(field_name)
            .expect("Field name stems from the set of fields.");
        if EncodingName::try_from_data_type(lhs_field.data_type()) != Some(EncodingName::PlainTerm)
        {
            return plan_err!("Join variables must have the PlainTermEncoding.");
        }

        let rhs_field = rhs
            .schema()
            .field_with_unqualified_name(field_name)
            .expect("Field name stems from the set of fields.");
        if EncodingName::try_from_data_type(rhs_field.data_type()) != Some(EncodingName::PlainTerm)
        {
            return plan_err!("Join variables must have the PlainTermEncoding.");
        }
    }

    Ok(())
}

/// TODO
fn compute_schema(
    join_type: SparqlJoinType,
    lhs: &LogicalPlan,
    rhs: &LogicalPlan,
) -> DFResult<DFSchemaRef> {
    Ok(match join_type {
        SparqlJoinType::Inner => {
            let mut new_schema = lhs.schema().as_ref().clone();
            new_schema.merge(rhs.schema());
            Arc::new(new_schema)
        }
        SparqlJoinType::Left => {
            let optional_rhs_fields = rhs
                .schema()
                .fields()
                .iter()
                .map(|f| f.as_ref().clone().with_nullable(true))
                .collect::<Fields>();
            let rhs_schema = DFSchema::from_unqualified_fields(
                optional_rhs_fields,
                rhs.schema().metadata().clone(),
            )?;

            let mut new_schema = lhs.schema().as_ref().clone();
            new_schema.merge(&rhs_schema);
            Arc::new(new_schema)
        }
    })
}
