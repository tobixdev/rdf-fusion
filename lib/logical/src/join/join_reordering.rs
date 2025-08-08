use crate::join::{SparqlJoinNode, SparqlJoinType};
use crate::quad_pattern::QuadPatternNode;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, HashSet};
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::RdfFusionEncodings;
use rdf_fusion_model::{NamedNodePattern, TermPattern};
use std::sync::Arc;

/// An optimizer rule that reorders SPARQL joins to improve query performance.
///
/// This rule analyzes join patterns in SPARQL queries and reorders them based on
/// cardinality estimates and join selectivity to reduce the amount of intermediate
/// data processed during query execution.
///
/// This rule only reorders directly nested inner joins that have no filter. Here is
/// an example of a logical plan that can be re-orderd:
///
/// ```text
/// SparqlJoin:
///     SparqlJoin:
///         QuadPattern ...
///         QuadPattern ...
///     QuadPattern ...
/// ```
#[derive(Debug)]
pub struct SparqlJoinReorderingRule {
    encodings: RdfFusionEncodings,
}

impl OptimizerRule for SparqlJoinReorderingRule {
    fn name(&self) -> &str {
        "sparql-join-reordering"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform_down(|plan| {
            let new_plan = match plan {
                LogicalPlan::Extension(Extension { node }) => {
                    if let Some(node) = node.as_any().downcast_ref::<SparqlJoinNode>() {
                        let new_plan = self.reorder_sparql_join(node, config)?;
                        Transformed::yes(new_plan)
                    } else {
                        Transformed::no(LogicalPlan::Extension(Extension { node }))
                    }
                }
                _ => Transformed::no(plan),
            };
            Ok(new_plan)
        })
    }
}

impl SparqlJoinReorderingRule {
    /// Creates a [SparqlJoinReorderingRule].
    pub fn new(encodings: RdfFusionEncodings) -> Self {
        Self { encodings }
    }

    /// Reorders a SPARQL join to optimize query execution.
    ///
    /// This is done in two steps:
    /// - Identifying the re-orderable components of the nested SPARQL joins, including
    ///   separating between connected components (i.e., overlapping variables)
    /// - Reordering the elements of each connected component, transforming it to a
    ///   [LogicalPlan], and combining them via a cross-join.
    fn reorder_sparql_join(
        &self,
        node: &SparqlJoinNode,
        config: &dyn OptimizerConfig,
    ) -> DFResult<LogicalPlan> {
        // See restrictions in JoinComponents.
        if node.filter().is_some() || node.join_type() != SparqlJoinType::Inner {
            return Ok(LogicalPlan::Extension(Extension {
                node: Arc::new(node.clone()),
            }));
        }

        let (lhs, rhs, _, _) = node.clone().destruct();
        let components = identify_join_components(lhs, rhs)
            .0
            .into_iter()
            .map(|connected_component| {
                // Apply rewrite to inner components
                let rewritten = connected_component
                    .0
                    .into_iter()
                    .map(|plan| Ok(self.rewrite(plan, config)?.data))
                    .collect::<DFResult<Vec<_>>>()?;
                let columns = extract_columns(&rewritten[0]);
                Ok(ConnectedJoinComponent(rewritten, columns))
            })
            .collect::<DFResult<Vec<_>>>()?;

        self.reorder_components(JoinComponents(components))
    }

    /// Tries to improve the join order by joining "cheap" nodes from the left side and pushing
    /// "expensive" parts of the join down on the right side.
    #[allow(clippy::expect_used)]
    fn reorder_components(&self, components: JoinComponents) -> DFResult<LogicalPlan> {
        components
            .0
            .into_iter()
            .map(|c| self.greedy_reorder_component(c))
            .reduce(|lhs, rhs| create_join(self.encodings.clone(), lhs?, rhs?))
            .expect("There must be at least one component")
    }

    /// Greedy reordering for a single connected component.
    #[allow(clippy::expect_used)]
    fn greedy_reorder_component(
        &self,
        component: ConnectedJoinComponent,
    ) -> DFResult<LogicalPlan> {
        /// Finds the next logical plan with the maximum cost that is connected to the current
        /// `used_vars`.
        fn pop_next_greedy(
            patterns: &mut Vec<LogicalPlan>,
            used_vars: &mut HashSet<Column>,
        ) -> LogicalPlan {
            // Find the next pattern that overlaps variables with 'used_vars'
            let mut worst: Option<(usize, usize)> = None;
            for (i, plan) in patterns.iter().enumerate() {
                let plan_vars = extract_columns(plan);
                if !used_vars.is_disjoint(&plan_vars) {
                    let cost = estimate_cardinality(plan);
                    if worst.is_none() || cost > worst.unwrap().1 {
                        worst = Some((i, cost));
                    }
                }
            }

            let (idx, _) = worst.expect(
                "There is always a join with overlapping variables in a non-empty component.",
            );
            patterns.remove(idx)
        }

        let mut to_order = component.0;
        let mut used_vars = HashSet::new();

        let (most_expensive_idx, _) = to_order
            .iter()
            .enumerate()
            .map(|(i, p)| (i, estimate_cardinality(p)))
            .max_by_key(|&(_, cost)| cost)
            .expect("There is at least one component");

        let mut current_plan = to_order.remove(most_expensive_idx);
        used_vars.extend(extract_columns(&current_plan));

        while !to_order.is_empty() {
            let next = pop_next_greedy(&mut to_order, &mut used_vars);
            used_vars.extend(extract_columns(&next));
            current_plan = create_join(self.encodings.clone(), next, current_plan)?;
        }

        Ok(current_plan)
    }
}

/// Represents a set of [ConnectedJoinComponent] that can be arbitrarily reordered.
///
/// Joins that cannot be reordered (yet):
/// - Joins with filters: We currently do not support handling filter scopes
/// - Left joins: We currently do not track the join type
#[derive(Clone, Debug)]
struct JoinComponents(Vec<ConnectedJoinComponent>);

impl JoinComponents {
    /// Merges two join components together.
    ///
    /// This checks whether the individual connected components share any variables and merges
    /// them if this is the case.
    pub fn merge(self, other: JoinComponents) -> Self {
        // Get all components
        let mut components = self.0;
        components.extend(other.0);

        // Merge components until all are pair-wise disjoint
        let mut changed = true;
        while changed {
            changed = false;
            for i in 0..components.len() {
                // TODO: Does one iteration suffice?
                let (left, right) = components.split_at_mut(i + 1);
                for right_element in right.iter_mut() {
                    if !left[i].1.is_disjoint(&right_element.1) {
                        // Merge j into i
                        left[i].0.extend(right_element.0.drain(0..));
                        left[i].1.extend(right_element.1.drain());

                        // Mark changed as true.
                        changed = true;
                    }
                }
            }
        }

        // Clean-up empty components
        components.retain(|component| !component.0.is_empty());

        JoinComponents(components)
    }
}

/// Represents a single connected join component.
///
/// Within a connected join component, the individual parts share variables with each other. If
/// two parts are part of the join component, they either share a variable directly or they share
/// a variable with another part of the component that then (transitively) shares a variable with
/// the other part.
#[derive(Clone, Debug)]
struct ConnectedJoinComponent(Vec<LogicalPlan>, HashSet<Column>);

/// Identifies a maximally large [JoinComponents] set.
///
/// The scope of the reordering will restrict itself to this set.
fn identify_join_components(lhs: LogicalPlan, rhs: LogicalPlan) -> JoinComponents {
    let lhs = identify_join_components_from_logical_plan(lhs);
    let rhs = identify_join_components_from_logical_plan(rhs);
    lhs.merge(rhs)
}

/// Either return the logical plan itself or, if the plan is a [SparqlJoinNode], recurses the
/// join component detection.
fn identify_join_components_from_logical_plan(plan: LogicalPlan) -> JoinComponents {
    let vars = extract_columns(&plan);
    match plan {
        LogicalPlan::Extension(extension) => {
            let node = extension.node.as_any();
            if let Some(node) = node.downcast_ref::<SparqlJoinNode>() {
                let (lhs, rhs, _, _) = node.clone().destruct();

                // Currently, we only support inner joins without filters.
                if node.filter().is_some() || node.join_type() != SparqlJoinType::Inner {
                    return JoinComponents(vec![ConnectedJoinComponent(
                        vec![LogicalPlan::Extension(extension)],
                        vars,
                    )]);
                }

                identify_join_components(lhs, rhs)
            } else {
                JoinComponents(vec![ConnectedJoinComponent(
                    vec![LogicalPlan::Extension(extension)],
                    vars,
                )])
            }
        }
        other => JoinComponents(vec![ConnectedJoinComponent(vec![other], vars)]),
    }
}

/// Estimates the cost of the join.
///
/// This uses the heuristics from Oxigraph's join reordering.
#[allow(clippy::expect_used, reason = "Very unrealistic / impossible")]
fn estimate_sparql_join_cost(lhs: &LogicalPlan, rhs: &LogicalPlan) -> usize {
    let number_of_common_vars = extract_columns(lhs)
        .intersection(&extract_columns(rhs))
        .count()
        .try_into()
        .expect("Unrealistically high variable count (> u32)");
    estimate_cardinality(lhs)
        .saturating_mul(estimate_cardinality(rhs))
        .saturating_div(1_000_usize.saturating_pow(number_of_common_vars))
}

/// Estimates the cost of a logical query plan.
///
/// "Cost" is here an abstract metric where a higher cost incurs a higher cost for computing the
/// result. While this is loosely tied to cardinality estimation, this optimization does currently
/// not use any statistics and relies on heuristics to estimate the cost.
fn estimate_cardinality(plan: &LogicalPlan) -> usize {
    match plan {
        LogicalPlan::Extension(ext) => {
            let node = ext.node.as_any();

            if let Some(node) = node.downcast_ref::<SparqlJoinNode>() {
                return estimate_sparql_join_cost(node.lhs(), node.rhs());
            }

            if let Some(node) = node.downcast_ref::<QuadPatternNode>() {
                return estimate_quad_cardinality(node);
            }

            usize::MAX
        }
        _ => usize::MAX,
    }
}

/// Estimates the cardinality of a single quad pattern.
///
/// This uses the heuristics from Oxigraph's join reordering.
fn estimate_quad_cardinality(quad: &QuadPatternNode) -> usize {
    let subject_bound = matches!(
        &quad.pattern().subject,
        TermPattern::NamedNode(_) | TermPattern::Literal(_)
    );
    let predicate_bound =
        matches!(&quad.pattern().predicate, NamedNodePattern::NamedNode(_));
    let object_bound = matches!(
        &quad.pattern().object,
        TermPattern::NamedNode(_) | TermPattern::Literal(_)
    );

    match (subject_bound, predicate_bound, object_bound) {
        (true, true, true) => 1,
        (true, true, false) => 10,
        (true, false, true) => 2,
        (false, true, true) => 10_000,
        (true, false, false) => 100,
        (false, false, false) => 1_000_000_000,
        (false, true, false) => 1_000_000,
        (false, false, true) => 100_000,
    }
}

/// Creates a [SparqlJoinNode] from two logical plans.
///
/// Currently, only one form of join is supported, so parameters are not needed.
fn create_join(
    encodings: RdfFusionEncodings,
    lhs: LogicalPlan,
    rhs: LogicalPlan,
) -> DFResult<LogicalPlan> {
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(SparqlJoinNode::try_new(
            encodings,
            lhs,
            rhs,
            None,
            SparqlJoinType::Inner,
        )?),
    }))
}

// Extracts all columns from a logical plan
fn extract_columns(plan: &LogicalPlan) -> HashSet<Column> {
    plan.schema()
        .fields()
        .iter()
        .map(|f| Column::new_unqualified(f.name()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::create_test_context;
    use crate::{ActiveGraph, RdfFusionLogicalPlanBuilderContext};
    use datafusion::logical_expr::col;
    use datafusion::optimizer::OptimizerContext;
    use datafusion::prelude::Expr;
    use insta::assert_snapshot;
    use rdf_fusion_model::{
        NamedNode, NamedNodePattern, TermPattern, TriplePattern, Variable,
    };

    #[test]
    fn basic_reordering_disconnected() {
        // cheap: all bound
        let a = quad(
            pat_named("http://s.org").into(),
            pat_named("http://p.org"),
            pat_named("http://o.org").into(),
        );

        // expensive: unbound subject/object
        let b = quad(pat_var("s"), pat_named("http://p.org".into()), pat_var("o"));

        // medium: only object bound
        let c = quad(
            pat_var("x"),
            pat_var_named("p"),
            pat_named("http://o.org".into()).into(),
        );

        let plan = join(join(a, b.clone()), c.clone());
        let result = reorder(plan);

        assert_snapshot!(result, @r"");
    }

    #[test]
    fn basic_reordering_connected() {
        let a = quad(
            pat_var("a").into(),
            pat_named("urn:p"),
            pat_named("urn:o").into(),
        );
        let b = quad(
            pat_var("b"),
            pat_named("urn:p".into()),
            pat_named("urn:o").into(),
        );
        let c = quad(
            pat_var("a"),
            pat_var_named("b"),
            pat_named("urn:o".into()).into(),
        );

        // Should be reordered such that no cross-join happens.

        let plan = join(join(a, b.clone()), c.clone());
        let result = reorder(plan);

        assert_snapshot!(result, @r"");
    }

    #[test]
    fn reorder_two_disconnected_components() {
        let a = quad(pat_var("x"), pat_named("urn:p1"), pat_var("y"));
        let b = quad(pat_var("a"), pat_named("urn:p2"), pat_var("b"));
        let c = quad(pat_var("x"), pat_named("urn:p3"), pat_var("z"));
        let d = quad(pat_var("b"), pat_named("urn:p4"), pat_var("c"));

        // Two join groups: (a, c) and (b, d)
        let join1 = join(join(a, b), join(c, d));

        let result = reorder(join1);

        assert_snapshot!(result, @r"");
    }

    #[test]
    fn reorder_large_connected_component() {
        let a = quad(pat_var("x"), pat_named("urn:p1"), pat_var("y"));
        let b = quad(pat_var("a"), pat_named("urn:p2"), pat_var("b"));
        let c = quad(pat_var("x"), pat_named("urn:p3"), pat_var("z"));
        let d = quad(pat_var("b"), pat_named("urn:p4"), pat_var("c"));
        let e = quad(pat_var("b"), pat_var_named("x"), pat_named("urn:p4").into());

        let result = reorder(join(join(join(a, b), join(c, d)), e));

        assert_snapshot!(result, @r"");
    }

    #[test]
    fn test_nested_joins_with_filter_skipped() {
        let a = quad(
            pat_var("a").into(),
            pat_named("urn:p"),
            pat_named("urn:o").into(),
        );
        let b = quad(
            pat_var("b"),
            pat_named("urn:p".into()),
            pat_named("urn:o").into(),
        );
        let c = quad(
            pat_var("a"),
            pat_var_named("b"),
            pat_named("urn:o".into()).into(),
        );

        let filtered_join =
            join_detailed(a, b, SparqlJoinType::Inner, Some(col("a").gt(col("b"))));
        let join = join(filtered_join, c);

        // Reordering shouldn't happen. Without the filter "c" would be the deep-right plan.
        let result = reorder(join);

        assert_snapshot!(result, @r"");
    }

    #[test]
    fn test_left_join_skipped() {
        let a = quad(
            pat_var("a").into(),
            pat_named("urn:p"),
            pat_named("urn:o").into(),
        );
        let b = quad(
            pat_var("b"),
            pat_named("urn:p".into()),
            pat_named("urn:o").into(),
        );
        let c = quad(
            pat_var("a"),
            pat_var_named("b"),
            pat_named("urn:o".into()).into(),
        );

        let filtered_join = join_detailed(a, b, SparqlJoinType::Left, None);
        let join = join(filtered_join, c);

        // Reordering shouldn't happen. Without the filter "c" would be the deep-right plan.
        let result = reorder(join);

        assert_snapshot!(result, @r"");
    }

    fn pat_var(name: &str) -> TermPattern {
        TermPattern::Variable(Variable::new(name.to_string()).unwrap())
    }

    fn pat_var_named(name: &str) -> NamedNodePattern {
        NamedNodePattern::Variable(Variable::new(name.to_string()).unwrap())
    }

    fn pat_named(name: &str) -> NamedNodePattern {
        NamedNodePattern::NamedNode(NamedNode::new(name.to_string()).unwrap())
    }

    fn quad(
        subject: TermPattern,
        predicate: NamedNodePattern,
        object: TermPattern,
    ) -> LogicalPlan {
        let context = create_test_context();
        RdfFusionLogicalPlanBuilderContext::new(context)
            .create_pattern(
                ActiveGraph::DefaultGraph,
                None,
                TriplePattern {
                    subject,
                    predicate,
                    object,
                },
            )
            .build()
            .unwrap()
    }

    fn join(lhs: LogicalPlan, rhs: LogicalPlan) -> LogicalPlan {
        join_detailed(lhs, rhs, SparqlJoinType::Inner, None)
    }

    fn join_detailed(
        lhs: LogicalPlan,
        rhs: LogicalPlan,
        join_type: SparqlJoinType,
        filter: Option<Expr>,
    ) -> LogicalPlan {
        let context = create_test_context();
        RdfFusionLogicalPlanBuilderContext::new(context)
            .create(Arc::new(lhs))
            .join(rhs, join_type, filter)
            .unwrap()
            .build()
            .unwrap()
    }

    fn reorder(plan: LogicalPlan) -> LogicalPlan {
        let rule =
            SparqlJoinReorderingRule::new(create_test_context().encodings().clone());
        rule.rewrite(plan, &OptimizerContext::new()).unwrap().data
    }
}
