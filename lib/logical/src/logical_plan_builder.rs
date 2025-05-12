use crate::active_graph::ActiveGraph;
use crate::extend::ExtendNode;
use crate::join::{SparqlJoinNode, SparqlJoinType};
use crate::paths::PropertyPathNode;
use crate::patterns::PatternNode;
use crate::quads::QuadsNode;
use crate::{DFResult, GraphFusionExprBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{plan_datafusion_err, Column, DFSchema, DFSchemaRef, JoinType};
use datafusion::logical_expr::{
    and, col, lit, Expr, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder, SortExpr,
    UserDefinedLogicalNode, Values,
};
use graphfusion_encoding::plain_term::encoders::DefaultPlainTermEncoder;
use graphfusion_encoding::plain_term::PlainTermEncoding;
use graphfusion_encoding::{EncodingName, EncodingScalar, TermEncoder, TermEncoding};
use graphfusion_functions::registry::GraphFusionFunctionRegistryRef;
use graphfusion_model::{NamedNode, Subject, Term, TermRef, ThinError, Variable};
use spargebra::algebra::PropertyPathExpression;
use spargebra::term::{GroundTerm, TermPattern, TriplePattern};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
// TODO: check types

#[derive(Debug, Clone)]
pub struct GraphFusionLogicalPlanBuilder {
    /// TODO
    plan_builder: LogicalPlanBuilder,
    /// TODO
    registry: GraphFusionFunctionRegistryRef,
}

impl GraphFusionLogicalPlanBuilder {
    /// TODO
    pub fn new(plan: Arc<LogicalPlan>, registry: GraphFusionFunctionRegistryRef) -> Self {
        let plan_builder = LogicalPlanBuilder::new_from_arc(plan);
        Self {
            plan_builder,
            registry,
        }
    }

    /// TODO
    pub fn new_from_quads(
        registry: GraphFusionFunctionRegistryRef,
        active_graph: ActiveGraph,
        subject: Option<Subject>,
        predicate: Option<NamedNode>,
        object: Option<Term>,
    ) -> Self {
        let node = QuadsNode::new(active_graph, subject, predicate, object);
        Self {
            plan_builder: create_extension_plan(node),
            registry,
        }
    }

    /// TODO
    pub fn new_with_empty_solution(registry: GraphFusionFunctionRegistryRef) -> Self {
        let plan_builder = LogicalPlanBuilder::empty(true);
        Self {
            plan_builder,
            registry,
        }
    }

    /// Creates a new [GraphFusionLogicalPlanBuilder] that holds the given VALUES as RDF terms.
    ///
    /// The [PlainTermEncoding] is used for encoding the terms.
    pub fn new_from_values(
        registry: GraphFusionFunctionRegistryRef,
        variables: &[Variable],
        bindings: &[Vec<Option<GroundTerm>>],
    ) -> DFResult<Self> {
        let fields = variables
            .iter()
            .map(|v| Field::new(v.as_str(), PlainTermEncoding::data_type(), true))
            .collect::<Fields>();
        let schema = DFSchema::from_unqualified_fields(fields, HashMap::new())?;

        if bindings.is_empty() {
            let empty =
                DefaultPlainTermEncoder::encode_term(ThinError::expected())?.into_scalar_value();
            let values_node = LogicalPlan::Values(Values {
                schema: Arc::new(schema),
                values: vec![vec![lit(empty); variables.len()]],
            });
            return Ok(Self {
                plan_builder: LogicalPlanBuilder::new(values_node),
                registry,
            });
        }

        let mut encoders = Vec::new();
        for _ in variables {
            encoders.push(Vec::new());
        }

        for solution in bindings {
            for (vec, term) in encoders.iter_mut().zip(solution.iter()) {
                let literal = DefaultPlainTermEncoder::encode_term(match term {
                    None => ThinError::expected(),
                    Some(term) => Ok(match term {
                        GroundTerm::NamedNode(nn) => TermRef::NamedNode(nn.as_ref()),
                        GroundTerm::Literal(lit) => TermRef::Literal(lit.as_ref()),
                    }),
                })?
                .into_scalar_value();
                vec.push(lit(literal));
            }
        }

        let values_node = LogicalPlan::Values(Values {
            schema: Arc::new(schema),
            values: encoders,
        });
        Ok(Self {
            plan_builder: LogicalPlanBuilder::new(values_node),
            registry,
        })
    }

    /// Creates a new [GraphFusionLogicalPlanBuilder] that matches the given basic graph pattern
    /// and returns all solutions.
    ///
    /// # Relevant Specifications
    /// - [SPARQL 1.1 - Basic Graph Patterns](https://www.w3.org/TR/sparql11-query/#BasicGraphPatterns)
    pub fn new_from_bgp(
        registry: GraphFusionFunctionRegistryRef,
        active_graph: &ActiveGraph,
        graph_variables: Option<&Variable>,
        patterns: &[TriplePattern],
    ) -> DFResult<GraphFusionLogicalPlanBuilder> {
        patterns
            .iter()
            .map(|p| {
                Self::new_from_pattern(
                    Arc::clone(&registry),
                    active_graph.clone(),
                    graph_variables.cloned(),
                    p.clone(),
                )
            })
            .reduce(|lhs, rhs| lhs?.join(rhs?.build()?, SparqlJoinType::Inner, None))
            .unwrap_or_else(|| {
                Ok(GraphFusionLogicalPlanBuilder::new_with_empty_solution(
                    Arc::clone(&registry),
                ))
            })
    }

    /// TODO
    pub fn new_from_pattern(
        registry: GraphFusionFunctionRegistryRef,
        active_graph: ActiveGraph,
        graph_variables: Option<Variable>,
        pattern: TriplePattern,
    ) -> DFResult<Self> {
        let quad = QuadsNode::new(active_graph, None, None, None);

        let quads_plan = create_extension_plan(quad).build()?;
        let pattern = PatternNode::try_new(
            quads_plan,
            vec![
                graph_variables.map(TermPattern::Variable),
                Some(pattern.subject),
                Some(pattern.predicate.into()),
                Some(pattern.object),
            ],
        )?;

        Ok(Self {
            plan_builder: create_extension_plan(pattern),
            registry,
        })
    }

    /// TODO
    pub fn new_from_property_path(
        registry: GraphFusionFunctionRegistryRef,
        active_graph: ActiveGraph,
        graph_variable: Option<Variable>,
        path: PropertyPathExpression,
        subject: TermPattern,
        object: TermPattern,
    ) -> DFResult<GraphFusionLogicalPlanBuilder> {
        let node = PropertyPathNode::new(
            active_graph,
            graph_variable.clone(),
            subject.clone(),
            path.clone(),
            object.clone(),
        )?;
        Ok(Self {
            registry,
            plan_builder: create_extension_plan(node),
        })
    }

    /// TODO
    pub fn project(self, variables: &[Variable]) -> DFResult<Self> {
        let plan_builder = self.plan_builder.project(
            variables
                .iter()
                .map(|v| col(Column::new_unqualified(v.as_str()))),
        )?;
        Ok(Self {
            plan_builder,
            registry: self.registry.clone(),
        })
    }

    /// Applies a filter using `expression`.
    ///
    /// TODO talk about handling fo terms
    pub fn filter(self, expression: Expr) -> DFResult<GraphFusionLogicalPlanBuilder> {
        let (datatype, _) = expression.data_type_and_nullable(self.schema())?;
        let expression = match datatype {
            // If the expression already evaluates to a Boolean, we can use it directly.
            DataType::Boolean => expression,
            // Otherwise, obtain the EBV. This will trigger an error on an unknown encoding.
            _ => self.expr_builder().effective_boolean_value(expression)?,
        };

        Ok(Self {
            registry: self.registry,
            plan_builder: self.plan_builder.filter(expression)?,
        })
    }

    /// TODO
    pub fn extend(self, variable: Variable, expr: Expr) -> DFResult<GraphFusionLogicalPlanBuilder> {
        let inner = self.plan_builder.build()?;
        let extend_node = ExtendNode::try_new(inner, variable, expr)?;
        Ok(Self {
            registry: self.registry,
            plan_builder: create_extension_plan(extend_node),
        })
    }

    /// Creates a join node of two logical plans that contain encoded RDF Terms.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Compatible Mappings](https://www.w3.org/TR/sparql11-query/#defn_algCompatibleMapping)
    pub fn join(
        self,
        rhs: LogicalPlan,
        join_type: SparqlJoinType,
        filter: Option<Expr>,
    ) -> DFResult<GraphFusionLogicalPlanBuilder> {
        let lhs = self.plan_builder.build()?;
        let join_node = SparqlJoinNode::try_new(lhs, rhs, filter, join_type)?;
        Ok(Self {
            registry: self.registry,
            plan_builder: LogicalPlanBuilder::new(LogicalPlan::Extension(Extension {
                node: Arc::new(join_node),
            })),
        })
    }

    /// Creates a limit node that applies skip (`start`) and fetch (`length`) to `inner`.
    pub fn slice(
        self,
        start: usize,
        length: Option<usize>,
    ) -> DFResult<GraphFusionLogicalPlanBuilder> {
        Ok(Self {
            registry: self.registry,
            plan_builder: self.plan_builder.limit(start, length)?,
        })
    }

    /// TODO
    pub fn order_by(self, exprs: &[SortExpr]) -> DFResult<GraphFusionLogicalPlanBuilder> {
        Ok(Self {
            registry: self.registry,
            plan_builder: self.plan_builder.sort(exprs.to_vec())?,
        })
    }

    /// TODO
    pub fn union(self, rhs: LogicalPlan) -> DFResult<GraphFusionLogicalPlanBuilder> {
        // TODO check types

        let mut new_schema = self.schema().as_ref().clone();
        new_schema.merge(rhs.schema().as_ref());

        let null = DefaultPlainTermEncoder::encode_term(ThinError::expected())?.into_scalar_value();
        let lhs_projections = new_schema
            .columns()
            .iter()
            .map(|c| {
                if self.schema().has_column(c) {
                    col(c.clone())
                } else {
                    lit(null.clone()).alias(c.name())
                }
            })
            .collect::<Vec<_>>();
        let rhs_projections = new_schema
            .columns()
            .iter()
            .map(|c| {
                if rhs.schema().has_column(c) {
                    col(c.clone())
                } else {
                    lit(null.clone()).alias(c.name())
                }
            })
            .collect::<Vec<_>>();

        let rhs = LogicalPlanBuilder::new(rhs);
        let result = self
            .plan_builder
            .project(lhs_projections)?
            .union(rhs.project(rhs_projections)?.build()?)?;
        Ok(Self {
            registry: self.registry,
            plan_builder: result,
        })
    }

    /// TODO
    pub fn minus(self, rhs: LogicalPlan) -> DFResult<GraphFusionLogicalPlanBuilder> {
        let lhs_keys: HashSet<_> = self
            .schema()
            .columns()
            .into_iter()
            .map(|c| c.name().to_owned())
            .collect();
        let rhs_keys: HashSet<_> = rhs
            .schema()
            .columns()
            .into_iter()
            .map(|c| c.name().to_owned())
            .collect();

        let overlapping_keys = lhs_keys
            .intersection(&rhs_keys)
            .collect::<HashSet<&String>>();
        if overlapping_keys.is_empty() {
            return Ok(self);
        }

        let lhs = self.plan_builder.alias("lhs")?;
        let rhs = LogicalPlanBuilder::new(rhs).alias("rhs")?;

        let mut join_schema = lhs.schema().as_ref().clone();
        join_schema.merge(rhs.schema());

        let expr_builder = GraphFusionExprBuilder::new(&join_schema, self.registry.as_ref());
        let mut join_filters = Vec::new();

        for k in &overlapping_keys {
            let expr = expr_builder.is_compatible(
                Expr::from(Column::new(Some("lhs"), *k)),
                Expr::from(Column::new(Some("rhs"), *k)),
            )?;
            join_filters.push(expr);
        }
        let any_both_not_null = overlapping_keys
            .iter()
            .map(|k| {
                and(
                    Expr::from(Column::new(Some("lhs"), *k)).is_not_null(),
                    Expr::from(Column::new(Some("rhs"), *k)).is_not_null(),
                )
            })
            .reduce(Expr::or)
            .ok_or(plan_datafusion_err!(
                "There must be at least one overlapping key"
            ))?;
        join_filters.push(any_both_not_null);

        let filter_expr = join_filters.into_iter().reduce(Expr::and);

        let projections = lhs_keys
            .iter()
            .map(|k| Expr::from(Column::new(Some("lhs"), k)).alias(k))
            .collect::<Vec<_>>();

        let plan = lhs
            .join_detailed(
                rhs.build()?,
                JoinType::LeftAnti,
                (Vec::<Column>::new(), Vec::<Column>::new()),
                filter_expr,
                false,
            )?
            .project(projections)?;
        Ok(Self {
            registry: self.registry,
            plan_builder: plan,
        })
    }

    /// TODO
    pub fn group(
        self,
        variables: &[Variable],
        aggregates: &[(Variable, Expr)],
    ) -> DFResult<GraphFusionLogicalPlanBuilder> {
        let expr_builder = self.expr_builder();
        let group_expr = variables
            .iter()
            .map(|v| expr_builder.variable(v.as_ref()))
            .collect::<DFResult<Vec<_>>>()?;
        let aggr_expr = aggregates
            .iter()
            .map(|(v, e)| e.clone().alias(v.as_str()))
            .collect::<Vec<_>>();

        // TODO: Ensure that aggr_expr is a term

        Ok(Self {
            registry: self.registry,
            plan_builder: self.plan_builder.aggregate(group_expr, aggr_expr)?,
        })
    }

    /// TODO
    pub fn distinct(self) -> DFResult<GraphFusionLogicalPlanBuilder> {
        self.distinct_with_sort(Vec::new())
    }

    /// TODO
    pub fn distinct_with_sort(
        self,
        sorts: Vec<SortExpr>,
    ) -> DFResult<GraphFusionLogicalPlanBuilder> {
        let schema = self.plan_builder.schema();
        let (on_expr, sorts) = create_distinct_on_expressions(self.expr_builder(), sorts.clone())?;
        let select_expr = schema.columns().into_iter().map(col).collect();
        let sorts = if sorts.is_empty() { None } else { Some(sorts) };

        Ok(Self {
            registry: self.registry,
            plan_builder: self.plan_builder.distinct_on(on_expr, select_expr, sorts)?,
        })
    }

    /// TODO
    pub fn with_plain_terms(self) -> DFResult<GraphFusionLogicalPlanBuilder> {
        let expr_builder = self.expr_builder();
        let with_correct_encoding = self
            .schema()
            .columns()
            .into_iter()
            .map(|c| {
                expr_builder
                    .with_encoding(col(c.clone()), EncodingName::PlainTerm)
                    .map(|e| e.alias(c.name()))
            })
            .collect::<DFResult<Vec<_>>>()?;
        Ok(Self {
            registry: self.registry,
            plan_builder: self.plan_builder.project(with_correct_encoding)?,
        })
    }

    /// TODO
    pub fn schema(&self) -> &DFSchemaRef {
        self.plan_builder.schema()
    }

    /// TODO
    pub fn expr_builder(&self) -> GraphFusionExprBuilder<'_> {
        let schema = self.schema().as_ref();
        GraphFusionExprBuilder::new(schema, self.registry.as_ref())
    }

    /// TODO
    pub fn into_inner(self) -> LogicalPlanBuilder {
        self.plan_builder
    }

    /// TODO
    pub fn build(self) -> DFResult<LogicalPlan> {
        self.plan_builder.build()
    }
}

/// TODO
fn create_distinct_on_expressions(
    expr_builder: GraphFusionExprBuilder<'_>,
    mut sort_expr: Vec<SortExpr>,
) -> DFResult<(Vec<Expr>, Vec<SortExpr>)> {
    let mut on_expr = sort_expr
        .iter()
        .map(|se| se.expr.clone())
        .collect::<Vec<_>>();

    for column in expr_builder.schema().columns() {
        let expr = col(column.clone());
        let sortable_expr = expr_builder.with_encoding(expr.clone(), EncodingName::Sortable)?;

        // If, initially, the sortable expression is already part of on_expr we don't re-add it.
        if !on_expr.contains(&sortable_expr) {
            on_expr.push(expr.clone());
            sort_expr.push(SortExpr::new(expr, true, true))
        }
    }

    Ok((on_expr, sort_expr))
}

/// TODO
fn create_extension_plan(node: impl UserDefinedLogicalNode + 'static) -> LogicalPlanBuilder {
    LogicalPlanBuilder::new(LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    }))
}
