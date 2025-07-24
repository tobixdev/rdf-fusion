use crate::active_graph::ActiveGraph;
use crate::join::SparqlJoinType;
use crate::paths::PropertyPathNode;
use crate::quad_pattern::QuadPatternNode;
use crate::RdfFusionLogicalPlanBuilder;
use datafusion::arrow::datatypes::{Field, Fields};
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::logical_expr::builder::project;
use datafusion::logical_expr::select_expr::SelectExpr;
use datafusion::logical_expr::{
    col, lit, Expr, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode, Values,
};
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::plain_term::encoders::DefaultPlainTermEncoder;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::{EncodingScalar, QuadStorageEncoding, TermEncoder, TermEncoding};
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
pub struct RdfFusionLogicalPlanBuilderContext {
    /// The registry allows us to access the registered functions. This is necessary for
    /// creating expressions within the builder.
    registry: RdfFusionFunctionRegistryRef,
    /// The encoding used by the storage layer.
    storage_encoding: QuadStorageEncoding,
}

impl RdfFusionLogicalPlanBuilderContext {
    /// Creates a new [RdfFusionLogicalPlanBuilder] with an existing `plan`.
    pub fn new(
        registry: RdfFusionFunctionRegistryRef,
        storage_encoding: QuadStorageEncoding,
    ) -> Self {
        Self {
            registry,
            storage_encoding,
        }
    }

    /// Returns a reference to the [RdfFusionFunctionRegistry](rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry)
    /// of the builder.
    pub fn registry(&self) -> &RdfFusionFunctionRegistryRef {
        &self.registry
    }

    /// Returns the [QuadStorageEncoding] of the builder.
    pub fn encoding(&self) -> &QuadStorageEncoding {
        &self.storage_encoding
    }

    /// Creates a new [RdfFusionLogicalPlanBuilder] with the given `plan`.
    #[allow(clippy::expect_used, reason = "Indicates programming error")]
    pub fn create(&self, plan: Arc<LogicalPlan>) -> RdfFusionLogicalPlanBuilder {
        RdfFusionLogicalPlanBuilder::new(self.clone(), plan)
    }

    /// Creates a new [RdfFusionLogicalPlanBuilder] that matches Quads.
    ///
    /// The `active_graph` dictates which graphs should be considered, while the optional constants
    /// (`subject`, `predicate`, `object`) allow filtering the resulting solution sequence.
    ///
    /// This does not allow you to bind values to variables. See [Self::create_pattern] for
    /// this purpose.
    #[allow(clippy::expect_used, reason = "Indicates programming error")]
    pub fn create_matching_quads(
        &self,
        active_graph: ActiveGraph,
        subject: Option<Subject>,
        predicate: Option<NamedNode>,
        object: Option<Term>,
    ) -> RdfFusionLogicalPlanBuilder {
        let partial_quads = self.create_pattern_node_from_constants(
            active_graph,
            subject.clone(),
            predicate.clone(),
            object.clone(),
        );
        let filled_quads =
            Self::fill_quads_with_constants(partial_quads, subject, predicate, object)
                .expect("Variables are fixed, Terms are encodable");

        RdfFusionLogicalPlanBuilder::new(self.clone(), Arc::new(filled_quads))
    }

    /// Creates a pattern node for the constant values provided.
    ///
    /// If a constant is `None`, the default name of the column (e.g., `?subject`) is used for the
    /// pattern.
    fn create_pattern_node_from_constants(
        &self,
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
            self.storage_encoding.clone(),
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
    #[allow(clippy::expect_used)]
    pub fn create_empty_solution(&self) -> RdfFusionLogicalPlanBuilder {
        let plan = LogicalPlanBuilder::empty(true)
            .build()
            .expect("Empty can always be built");
        RdfFusionLogicalPlanBuilder::new(self.clone(), Arc::new(plan))
    }

    /// Creates a new [RdfFusionLogicalPlanBuilder] that holds the given VALUES as RDF terms.
    ///
    /// The [PlainTermEncoding](rdf_fusion_encoding::plain_term::PlainTermEncoding) is used for
    /// encoding the terms.
    pub fn create_values(
        &self,
        variables: &[Variable],
        bindings: &[Vec<Option<GroundTerm>>],
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        let fields = variables
            .iter()
            .map(|v| Field::new(v.as_str(), PLAIN_TERM_ENCODING.data_type(), true))
            .collect::<Fields>();
        let schema = DFSchema::from_unqualified_fields(fields, HashMap::new())?;

        if bindings.is_empty() {
            let empty =
                DefaultPlainTermEncoder::encode_term(ThinError::expected())?.into_scalar_value();
            let plan = LogicalPlanBuilder::values_with_schema(
                vec![vec![lit(empty); variables.len()]],
                &Arc::new(schema),
            )?
            .build()?;
            return Ok(RdfFusionLogicalPlanBuilder::new(
                self.clone(),
                Arc::new(plan),
            ));
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
        Ok(RdfFusionLogicalPlanBuilder::new(
            self.clone(),
            Arc::new(values_node),
        ))
    }

    /// Creates a new [RdfFusionLogicalPlanBuilder] that matches the given basic graph pattern
    /// and returns all solutions.
    ///
    /// # Relevant Specifications
    /// - [SPARQL 1.1 - Basic Graph Patterns](https://www.w3.org/TR/sparql11-query/#BasicGraphPatterns)
    pub fn create_bgp(
        &self,
        active_graph: &ActiveGraph,
        graph_variables: Option<&Variable>,
        patterns: &[TriplePattern],
    ) -> DFResult<RdfFusionLogicalPlanBuilder> {
        patterns
            .iter()
            .map(|p| self.create_pattern(active_graph.clone(), graph_variables.cloned(), p.clone()))
            .map(Ok)
            .reduce(|lhs, rhs| lhs?.join(rhs?.build()?, SparqlJoinType::Inner, None))
            .unwrap_or_else(|| Ok(self.create_empty_solution()))
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
    pub fn create_pattern(
        &self,
        active_graph: ActiveGraph,
        graph_variable: Option<Variable>,
        pattern: TriplePattern,
    ) -> RdfFusionLogicalPlanBuilder {
        let quads = QuadPatternNode::new(
            self.storage_encoding.clone(),
            active_graph,
            graph_variable,
            pattern,
        );
        RdfFusionLogicalPlanBuilder::new(self.clone(), create_extension_plan(quads))
    }

    /// Creates a new [RdfFusionLogicalPlanBuilder] from a SPARQL [PropertyPathExpression].
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Property Paths](https://www.w3.org/TR/sparql11-query/#propertypaths)
    pub fn create_property_path(
        &self,
        active_graph: ActiveGraph,
        graph_variable: Option<Variable>,
        path: PropertyPathExpression,
        subject: TermPattern,
        object: TermPattern,
    ) -> RdfFusionLogicalPlanBuilder {
        let node = PropertyPathNode::new(active_graph, graph_variable, subject, path, object);
        RdfFusionLogicalPlanBuilder::new(self.clone(), create_extension_plan(node))
    }
}

/// Creates a `LogicalPlanBuilder` from a user-defined logical node.
fn create_extension_plan(node: impl UserDefinedLogicalNode + 'static) -> Arc<LogicalPlan> {
    Arc::new(LogicalPlan::Extension(Extension {
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
