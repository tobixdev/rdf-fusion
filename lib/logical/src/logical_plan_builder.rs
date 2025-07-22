use crate::active_graph::ActiveGraph;
use crate::extend::ExtendNode;
use crate::join::{compute_sparql_join_columns, SparqlJoinNode, SparqlJoinType};
use crate::minus::MinusNode;
use crate::paths::PropertyPathNode;
use crate::quad_pattern::QuadPatternNode;
use crate::{RdfFusionExprBuilder, RdfFusionExprBuilderRoot};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{Column, DFSchema, DFSchemaRef, DataFusionError};
use datafusion::logical_expr::builder::project;
use datafusion::logical_expr::select_expr::SelectExpr;
use datafusion::logical_expr::{
    col, lit, Expr, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder, Sort, SortExpr,
    UserDefinedLogicalNode, Values,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::plain_term::encoders::DefaultPlainTermEncoder;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::typed_value::DEFAULT_QUAD_DFSCHEMA;
use rdf_fusion_encoding::{
    EncodingName, EncodingScalar, TermEncoder, TermEncoding, COL_GRAPH, COL_OBJECT, COL_PREDICATE,
    COL_SUBJECT,
};
use rdf_fusion_functions::registry::RdfFusionFunctionRegistryRef;
use rdf_fusion_model::{
    GroundTerm, NamedNode, NamedNodePattern, PropertyPathExpression, Subject, Term, TermPattern,
    TermRef, ThinError, TriplePattern, Variable,
};
use std::collections::HashMap;
use std::sync::Arc;

/// A convenient builder for programmatically creating SPARQL queries.
///
/// # Example
///
/// ```
/// use std::sync::Arc;
/// use datafusion::logical_expr::LogicalPlan;
/// use rdf_fusion_logical::RdfFusionLogicalPlanBuilder;
/// use rdf_fusion_functions::registry::{DefaultRdfFusionFunctionRegistry, RdfFusionFunctionRegistry};
/// use rdf_fusion_model::{TriplePattern, TermPattern, Variable, NamedNodePattern};
/// use rdf_fusion_logical::ActiveGraph;
///
/// let subject = Variable::new_unchecked("s");
/// let predicate = Variable::new_unchecked("p");
/// let object = Variable::new_unchecked("o");
///
/// let pattern = TriplePattern {
///     subject: TermPattern::Variable(subject.clone()),
///     predicate: NamedNodePattern::Variable(predicate),
///     object: TermPattern::Variable(object),
/// };
///
/// let pattern = RdfFusionLogicalPlanBuilder::new_from_pattern(
///     Arc::new(DefaultRdfFusionFunctionRegistry::default()),
///     ActiveGraph::default(),
///     None,
///     pattern,
/// );
/// let plan: LogicalPlan = pattern
///     .project(&[subject])
///     .unwrap()
///     .build()
///     .unwrap();
/// ```
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
    /// (`subject`, `predicate`, `object`) allow filtering the resulting solution sequence.
    ///
    /// This does not allow you to bind values to variables. See [Self::new_from_pattern] for
    /// this purpose.
    #[allow(clippy::expect_used, reason = "Indicates programming error")]
    pub fn new_from_matching_quads(
        registry: RdfFusionFunctionRegistryRef,
        active_graph: ActiveGraph,
        subject: Option<Subject>,
        predicate: Option<NamedNode>,
        object: Option<Term>,
    ) -> Self {
        let partial_quads = Self::create_pattern_node_from_constants(
            active_graph,
            subject.clone(),
            predicate.clone(),
            object.clone(),
        );
        let filled_quads =
            Self::fill_quads_with_constants(partial_quads, subject, predicate, object)
                .expect("Variables are fixed, Terms are encodable");

        assert_eq!(
            filled_quads.schema().as_ref(),
            DEFAULT_QUAD_DFSCHEMA.as_ref(),
            "Unexpected schema for matching quads."
        );

        Self {
            plan_builder: LogicalPlanBuilder::new(filled_quads),
            registry,
        }
    }

    /// Creates a pattern node for the constant values provided.
    ///
    /// If a constant is `None`, the default name of the column (e.g., `?subject`) is used for the
    /// pattern.
    fn create_pattern_node_from_constants(
        active_graph: ActiveGraph,
        subject: Option<Subject>,
        predicate: Option<NamedNode>,
        object: Option<Term>,
    ) -> QuadPatternNode {
        let triple_pattern = TriplePattern {
            subject: subject.map_or(
                TermPattern::Variable(Variable::new_unchecked(COL_SUBJECT)),
                |s| TermPattern::from(Term::from(s)),
            ),
            predicate: predicate.map_or(
                NamedNodePattern::Variable(Variable::new_unchecked(COL_PREDICATE)),
                NamedNodePattern::from,
            ),
            object: object.map_or(
                TermPattern::Variable(Variable::new_unchecked(COL_OBJECT)),
                TermPattern::from,
            ),
        };

        QuadPatternNode::new_with_blank_nodes_as_filter(
            active_graph,
            Some(Variable::new_unchecked(COL_GRAPH)),
            triple_pattern,
        )
    }

    /// Fills missing columns in the quads with the constants.
    fn fill_quads_with_constants(
        inner: QuadPatternNode,
        subject: Option<Subject>,
        predicate: Option<NamedNode>,
        object: Option<Term>,
    ) -> DFResult<LogicalPlan> {
        let graph = col(COL_GRAPH);
        let subject = column_or_literal(subject, COL_SUBJECT)?;
        let predicate = column_or_literal(predicate, COL_PREDICATE)?;
        let object = column_or_literal(object, COL_OBJECT)?;

        let inner = LogicalPlan::Extension(Extension {
            node: Arc::new(inner),
        });

        project(
            inner,
            [graph, subject, predicate, object]
                .into_iter()
                .map(SelectExpr::from),
        )
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
            .map(|v| Field::new(v.as_str(), PLAIN_TERM_ENCODING.data_type(), true))
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
            .map(Ok)
            .reduce(|lhs, rhs| lhs?.join(rhs?.build()?, SparqlJoinType::Inner, None))
            .unwrap_or_else(|| {
                Ok(RdfFusionLogicalPlanBuilder::new_with_empty_solution(
                    registry,
                ))
            })
    }

    /// Creates a new [RdfFusionLogicalPlanBuilder] that matches a single `pattern` on the
    /// `active_graph`.
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
        graph_variable: Option<Variable>,
        pattern: TriplePattern,
    ) -> Self {
        let quads = QuadPatternNode::new(active_graph, graph_variable, pattern);
        Self {
            plan_builder: create_extension_plan(quads),
            registry,
        }
    }

    /// Creates a new [RdfFusionLogicalPlanBuilder] from a SPARQL [PropertyPathExpression].
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Property Paths](https://www.w3.org/TR/sparql11-query/#propertypaths)
    pub fn new_from_property_path(
        registry: RdfFusionFunctionRegistryRef,
        active_graph: ActiveGraph,
        graph_variable: Option<Variable>,
        path: PropertyPathExpression,
        subject: TermPattern,
        object: TermPattern,
    ) -> RdfFusionLogicalPlanBuilder {
        let node = PropertyPathNode::new(active_graph, graph_variable, subject, path, object);
        Self {
            registry,
            plan_builder: create_extension_plan(node),
        }
    }

    /// Projects the current plan to a new set of variables.
    pub fn project(self, variables: &[Variable]) -> DFResult<Self> {
        let plan_builder = self.plan_builder.project(
            variables
                .iter()
                .map(|v| col(Column::new_unqualified(v.as_str()))),
        )?;
        Ok(Self {
            plan_builder,
            registry: Arc::clone(&self.registry),
        })
    }

    /// Applies a filter using `expression`.
    ///
    /// The filter expression is evaluated for each solution. If the effective boolean value of the
    /// expression is `true`, the solution is kept; otherwise, it is discarded.
    ///
    /// If the expression does not evaluate to a boolean, its effective boolean value is
    /// determined according to SPARQL rules.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Effective Boolean Value (EBV)](https://www.w3.org/TR/sparql11-query/#ebv)
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

    /// Extends the current plan with a new variable binding.
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

        let join_columns = compute_sparql_join_columns(self.schema(), rhs.schema())?;
        let any_non_plainterm = join_columns.iter().any(|(_, encodings)| {
            encodings.len() > 1 || encodings.iter().next() != Some(&EncodingName::PlainTerm)
        });

        let (lhs, rhs) = if any_non_plainterm {
            // TODO: maybe we can be more conservative here and only apply the plain term encoding
            // to the join columns
            let lhs = self.with_plain_terms()?.plan_builder.build()?;
            let rhs = Self::new(Arc::new(rhs), Arc::clone(&registry))
                .with_plain_terms()?
                .plan_builder
                .build()?;
            (lhs, rhs)
        } else {
            (self.plan_builder.build()?, rhs)
        };

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

    /// Sorts the current plan by a given set of expressions.
    pub fn order_by(self, exprs: &[SortExpr]) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let exprs = exprs
            .iter()
            .map(|sort| self.ensure_sortable(sort))
            .collect::<DFResult<Vec<_>>>()?;

        let registry = Arc::clone(&self.registry);
        let plan = LogicalPlan::Sort(Sort {
            input: Arc::new(self.build()?),
            expr: exprs,
            fetch: None,
        });

        Ok(Self {
            registry,
            plan_builder: LogicalPlanBuilder::new(plan),
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

    /// Creates a union of the current plan and another plan.
    pub fn union(self, rhs: LogicalPlan) -> DFResult<RdfFusionLogicalPlanBuilder> {
        // TODO check types

        let mut new_schema = self.schema().as_ref().clone();
        new_schema.merge(rhs.schema().as_ref());

        let rhs = LogicalPlanBuilder::new(rhs);
        let result = self.plan_builder.union_by_name(rhs.build()?)?;
        Ok(Self {
            registry: self.registry,
            plan_builder: result,
        })
    }

    /// Subtracts the results of another plan from the current plan.
    pub fn minus(self, rhs: LogicalPlan) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let minus_node = MinusNode::new(self.plan_builder.build()?, rhs);
        Ok(Self {
            registry: self.registry,
            plan_builder: create_extension_plan(minus_node),
        })
    }

    /// Groups the current plan by a set of variables and applies aggregate expressions.
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

    /// Removes duplicate solutions from the current plan.
    pub fn distinct(self) -> DFResult<RdfFusionLogicalPlanBuilder> {
        self.distinct_with_sort(Vec::new())
    }

    /// Removes duplicate solutions from the current plan, with additional sorting.
    pub fn distinct_with_sort(self, sorts: Vec<SortExpr>) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let schema = self.plan_builder.schema();
        let (on_expr, sorts) = create_distinct_on_expressions(self.expr_builder_root(), sorts)?;
        let select_expr = schema.columns().into_iter().map(col).collect();
        let sorts = if sorts.is_empty() { None } else { Some(sorts) };

        Ok(Self {
            registry: self.registry,
            plan_builder: self.plan_builder.distinct_on(on_expr, select_expr, sorts)?,
        })
    }

    /// Ensures all columns are encoded as plain terms.
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

    /// Returns the schema of the current plan.
    pub fn schema(&self) -> &DFSchemaRef {
        self.plan_builder.schema()
    }

    /// Returns the function registry.
    pub fn registry(&self) -> &RdfFusionFunctionRegistryRef {
        &self.registry
    }

    /// Consumes the builder and returns the inner `LogicalPlanBuilder`.
    pub fn into_inner(self) -> LogicalPlanBuilder {
        self.plan_builder
    }

    /// Builds the `LogicalPlan`.
    pub fn build(self) -> DFResult<LogicalPlan> {
        self.plan_builder.build()
    }

    /// Returns a new [RdfFusionExprBuilderRoot].
    pub fn expr_builder_root(&self) -> RdfFusionExprBuilderRoot<'_> {
        let schema = self.schema().as_ref();
        RdfFusionExprBuilderRoot::new(self.registry.as_ref(), schema)
    }

    /// Returns a new [RdfFusionExprBuilder] for a given expression.
    pub fn expr_builder(&self, expr: Expr) -> DFResult<RdfFusionExprBuilder<'_>> {
        self.expr_builder_root().try_create_builder(expr)
    }
}

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

/// Creates a `LogicalPlanBuilder` from a user-defined logical node.
fn create_extension_plan(node: impl UserDefinedLogicalNode + 'static) -> LogicalPlanBuilder {
    LogicalPlanBuilder::new(LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    }))
}

fn column_or_literal(term: Option<impl Into<Term>>, col_name: &str) -> DFResult<Expr> {
    Ok(term
        .map(|s| {
            Ok::<Expr, DataFusionError>(
                lit(PLAIN_TERM_ENCODING
                    .encode_term(Ok(s.into().as_ref()))?
                    .into_scalar_value())
                .alias(col_name),
            )
        })
        .transpose()?
        .unwrap_or(col(col_name)))
}
