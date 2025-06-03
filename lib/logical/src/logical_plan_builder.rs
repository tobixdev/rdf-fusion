use crate::active_graph::ActiveGraph;
use crate::extend::ExtendNode;
use crate::join::{SparqlJoinNode, SparqlJoinType};
use crate::minus::MinusNode;
use crate::paths::PropertyPathNode;
use crate::patterns::PatternNode;
use crate::quads::QuadsNode;
use crate::{DFResult, RdfFusionExprBuilder, RdfFusionExprBuilderRoot};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{Column, DFSchema, DFSchemaRef};
use datafusion::logical_expr::{
    col, lit, Expr, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder, SortExpr,
    UserDefinedLogicalNode, Values,
};
use rdf_fusion_encoding::plain_term::encoders::DefaultPlainTermEncoder;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::{EncodingName, EncodingScalar, TermEncoder, TermEncoding};
use rdf_fusion_functions::registry::RdfFusionFunctionRegistryRef;
use rdf_fusion_model::{NamedNode, Subject, Term, TermRef, ThinError, Variable};
use spargebra::algebra::PropertyPathExpression;
use spargebra::term::{GroundTerm, TermPattern, TriplePattern};
use std::collections::HashMap;
use std::sync::Arc;

/// A convenient builder for programmatically creating SPARQL queries.
///
/// TODO example
#[derive(Debug, Clone)]
pub struct RdfFusionLogicalPlanBuilder {
    /// The inner DataFusion [LogicalPlanBuilder].
    ///
    /// We do not use [LogicalPlan] directly as we want to leverage the convenience (and validation)
    /// that the [LogicalPlanBuilder] provides.
    plan_builder: LogicalPlanBuilder,
    /// The registry allows us to access the registered functions. This is necessary for
    /// creating expressions within the builder.
    registry: RdfFusionFunctionRegistryRef,
}

impl RdfFusionLogicalPlanBuilder {
    /// Creates a new [RdfFusionLogicalPlanBuilder] with an existing `plan`.
    pub fn new(plan: Arc<LogicalPlan>, registry: RdfFusionFunctionRegistryRef) -> Self {
        let plan_builder = LogicalPlanBuilder::new_from_arc(plan);
        Self {
            plan_builder,
            registry,
        }
    }

    /// Creates a new [RdfFusionLogicalPlanBuilder] that matches Quads.
    ///
    /// The `active_graph` dictates which graphs should be considered, while the optional constants
    /// (`subject`, `predicate`, `object`) allows filtering the resulting solution sequence.
    ///
    /// This does not allow you to bind values to custom variable. See [Self::new_from_pattern] for
    /// this purpose.
    pub fn new_from_quads(
        registry: RdfFusionFunctionRegistryRef,
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

    /// Creates a new [RdfFusionLogicalPlanBuilder] that that returns a single empty solution.
    pub fn new_with_empty_solution(registry: RdfFusionFunctionRegistryRef) -> Self {
        let plan_builder = LogicalPlanBuilder::empty(true);
        Self {
            plan_builder,
            registry,
        }
    }

    /// Creates a new [RdfFusionLogicalPlanBuilder] that holds the given VALUES as RDF terms.
    ///
    /// The [PlainTermEncoding] is used for encoding the terms.
    pub fn new_from_values(
        registry: RdfFusionFunctionRegistryRef,
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
            let plan_builder = LogicalPlanBuilder::values_with_schema(
                vec![vec![lit(empty); variables.len()]],
                &Arc::new(schema),
            )?;
            return Ok(Self {
                plan_builder,
                registry,
            });
        }

        let mut rows = Vec::new();
        for solution in bindings {
            let mut row = Vec::new();
            for term in solution {
                let literal = DefaultPlainTermEncoder::encode_term(match term {
                    None => ThinError::expected(),
                    Some(term) => Ok(match term {
                        GroundTerm::NamedNode(nn) => TermRef::NamedNode(nn.as_ref()),
                        GroundTerm::Literal(lit) => TermRef::Literal(lit.as_ref()),
                    }),
                })?
                .into_scalar_value();
                row.push(lit(literal));
            }
            rows.push(row);
        }

        let values_node = LogicalPlan::Values(Values {
            schema: Arc::new(schema),
            values: rows,
        });
        Ok(Self {
            plan_builder: LogicalPlanBuilder::new(values_node),
            registry,
        })
    }

    /// Creates a new [RdfFusionLogicalPlanBuilder] that matches the given basic graph pattern
    /// and returns all solutions.
    ///
    /// # Example
    ///
    /// TODO
    ///
    /// # Relevant Specifications
    /// - [SPARQL 1.1 - Basic Graph Patterns](https://www.w3.org/TR/sparql11-query/#BasicGraphPatterns)
    pub fn new_from_bgp(
        registry: RdfFusionFunctionRegistryRef,
        active_graph: &ActiveGraph,
        graph_variables: Option<&Variable>,
        patterns: &[TriplePattern],
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
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
                Ok(RdfFusionLogicalPlanBuilder::new_with_empty_solution(
                    Arc::clone(&registry),
                ))
            })
    }

    /// Creates a new [RdfFusionLogicalPlanBuilder] that matches a single `pattern` on the
    /// `active_graph`.
    ///
    /// # Example
    ///
    /// TODO
    ///
    /// # Active Graph
    ///
    /// The `active_graph` is interpreted from the viewpoint of the quad store, not the query. This
    /// API does not have knowledge about RDF data sets and it is up to the user to correctly
    /// construct an [ActiveGraph] instance from the data set.
    ///
    /// See [ActiveGraph] for more detailed information.
    pub fn new_from_pattern(
        registry: RdfFusionFunctionRegistryRef,
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
        registry: RdfFusionFunctionRegistryRef,
        active_graph: ActiveGraph,
        graph_variable: Option<Variable>,
        path: PropertyPathExpression,
        subject: TermPattern,
        object: TermPattern,
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
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
    pub fn filter(self, expression: Expr) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let (datatype, _) = expression.data_type_and_nullable(self.schema())?;
        let expression = match datatype {
            // If the expression already evaluates to a Boolean, we can use it directly.
            DataType::Boolean => expression,
            // Otherwise, obtain the EBV. This will trigger an error on an unknown encoding.
            _ => self
                .expr_builder(expression)?
                .build_effective_boolean_value()?,
        };

        Ok(Self {
            registry: self.registry,
            plan_builder: self.plan_builder.filter(expression)?,
        })
    }

    /// TODO
    pub fn extend(self, variable: Variable, expr: Expr) -> DFResult<RdfFusionLogicalPlanBuilder> {
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
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let registry = Arc::clone(&self.registry);

        // TODO: We could be more conservative here and only transform join variables.
        let lhs = self.with_plain_terms()?.plan_builder.build()?;
        let rhs = Self::new(Arc::new(rhs), Arc::clone(&registry))
            .with_plain_terms()?
            .plan_builder
            .build()?;

        let join_node = SparqlJoinNode::try_new(lhs, rhs, filter, join_type)?;
        Ok(Self {
            registry,
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
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        Ok(Self {
            registry: self.registry,
            plan_builder: self.plan_builder.limit(start, length)?,
        })
    }

    /// TODO
    pub fn order_by(self, exprs: &[SortExpr]) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let exprs = exprs
            .iter()
            .map(|sort| self.ensure_sortable(sort))
            .collect::<DFResult<Vec<_>>>()?;
        Ok(Self {
            registry: self.registry,
            plan_builder: self.plan_builder.sort(exprs)?,
        })
    }

    /// Ensure that the [EncodingName::Sortable] is used.
    fn ensure_sortable(&self, e: &SortExpr) -> DFResult<SortExpr> {
        let expr = self
            .expr_builder(e.expr.clone())?
            .with_encoding(EncodingName::Sortable)?
            .build()?;
        Ok(SortExpr::new(expr, e.asc, e.nulls_first))
    }

    /// TODO
    pub fn union(self, rhs: LogicalPlan) -> DFResult<RdfFusionLogicalPlanBuilder> {
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
    pub fn minus(self, rhs: LogicalPlan) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let minus_node = MinusNode::new(self.plan_builder.build()?, rhs)?;
        Ok(Self {
            registry: self.registry,
            plan_builder: create_extension_plan(minus_node),
        })
    }

    /// TODO
    pub fn group(
        self,
        variables: &[Variable],
        aggregates: &[(Variable, Expr)],
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let group_expr = variables
            .iter()
            .map(|v| self.create_group_expr(v))
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

    /// Creates an [Expr] that ensures that the grouped values uses an [EncodingName::PlainTerm]
    /// encoding.
    fn create_group_expr(&self, v: &Variable) -> DFResult<Expr> {
        Ok(self
            .expr_builder_root()
            .variable(v.as_ref())?
            .with_encoding(EncodingName::PlainTerm)?
            .build()?
            .alias(v.as_str()))
    }

    /// TODO
    pub fn distinct(self) -> DFResult<RdfFusionLogicalPlanBuilder> {
        self.distinct_with_sort(Vec::new())
    }

    /// TODO
    pub fn distinct_with_sort(self, sorts: Vec<SortExpr>) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let schema = self.plan_builder.schema();
        let (on_expr, sorts) =
            create_distinct_on_expressions(self.expr_builder_root(), sorts.clone())?;
        let select_expr = schema.columns().into_iter().map(col).collect();
        let sorts = if sorts.is_empty() { None } else { Some(sorts) };

        Ok(Self {
            registry: self.registry,
            plan_builder: self.plan_builder.distinct_on(on_expr, select_expr, sorts)?,
        })
    }

    /// TODO
    pub fn with_plain_terms(self) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let with_correct_encoding = self
            .schema()
            .columns()
            .into_iter()
            .map(|c| {
                let expr = self
                    .expr_builder(col(c.clone()))?
                    .with_encoding(EncodingName::PlainTerm)?
                    .build()?
                    .alias(c.name());
                Ok(expr)
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
    pub fn registry(&self) -> &RdfFusionFunctionRegistryRef {
        &self.registry
    }

    /// TODO
    pub fn into_inner(self) -> LogicalPlanBuilder {
        self.plan_builder
    }

    /// TODO
    pub fn build(self) -> DFResult<LogicalPlan> {
        self.plan_builder.build()
    }

    /// TODO
    pub fn expr_builder_root(&self) -> RdfFusionExprBuilderRoot<'_> {
        let schema = self.schema().as_ref();
        RdfFusionExprBuilderRoot::new(self.registry.as_ref(), schema)
    }

    /// TODO
    pub fn expr_builder(&self, expr: Expr) -> DFResult<RdfFusionExprBuilder<'_>> {
        self.expr_builder_root().try_create_builder(expr)
    }
}

/// TODO
fn create_distinct_on_expressions(
    expr_builder_root: RdfFusionExprBuilderRoot<'_>,
    mut sort_expr: Vec<SortExpr>,
) -> DFResult<(Vec<Expr>, Vec<SortExpr>)> {
    let mut on_expr = sort_expr
        .iter()
        .map(|se| se.expr.clone())
        .collect::<Vec<_>>();

    for column in expr_builder_root.schema().columns() {
        let expr = col(column.clone());
        let sortable_expr = expr_builder_root
            .try_create_builder(expr.clone())?
            .with_encoding(EncodingName::Sortable)?
            .build()?;

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
