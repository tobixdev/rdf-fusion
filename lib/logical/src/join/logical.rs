use datafusion::arrow::datatypes::{DataType, Fields};
use datafusion::common::{plan_datafusion_err, plan_err, DFSchema, DFSchemaRef};
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::EncodingName;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// TODO
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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

#[derive(Clone, PartialEq, Eq, Hash)]
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

    /// TODO
    pub fn destruct(self) -> (LogicalPlan, LogicalPlan, Option<Expr>, SparqlJoinType) {
        (self.lhs, self.rhs, self.filter, self.join_type)
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
    let join_column = compute_sparql_join_columns(lhs.schema(), rhs.schema())?;

    for (field_name, encodings) in join_column {
        if encodings.len() > 1 {
            return plan_err!("Join column '{field_name}' has multiple encodings.");
        }

        let encoding = encodings
            .into_iter()
            .next()
            .expect("Length already checked");
        if encoding != EncodingName::PlainTerm {
            return plan_err!("Join column '{field_name}' must have the PlainTermEncoding.");
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

/// Computes the columns that are being joined in a
pub fn compute_sparql_join_columns(
    lhs: &DFSchema,
    rhs: &DFSchema,
) -> DFResult<HashMap<String, HashSet<EncodingName>>> {
    /// Extracts the encoding of a field.
    ///
    /// It is expected that `name` is part of `schema`.
    #[allow(clippy::expect_used, reason = "Local function, Guarantees met below")]
    fn extract_encoding(schema: &DFSchema, name: &str) -> DFResult<EncodingName> {
        let field = schema
            .field_with_unqualified_name(name)
            .expect("Field name stems from the set of fields.");
        EncodingName::try_from_data_type(field.data_type()).ok_or(plan_datafusion_err!(
            "Field '{}' must be an RDF Term.",
            name
        ))
    }

    let lhs_fields = lhs
        .fields()
        .iter()
        .map(|f| f.name().to_owned())
        .collect::<HashSet<_>>();
    let rhs_fields = rhs
        .fields()
        .iter()
        .map(|f| f.name().to_owned())
        .collect::<HashSet<_>>();

    let mut result = HashMap::new();
    for field_name in lhs_fields.intersection(&rhs_fields) {
        let lhs_encoding = extract_encoding(lhs, field_name)?;
        let rhs_encoding = extract_encoding(rhs, field_name)?;
        result.insert(
            field_name.clone(),
            vec![lhs_encoding, rhs_encoding].into_iter().collect(),
        );
    }

    Ok(result)
}
