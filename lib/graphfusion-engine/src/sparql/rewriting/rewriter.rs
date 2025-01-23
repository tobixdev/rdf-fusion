use crate::results::decode_rdf_terms;
use crate::DFResult;
use arrow_rdf::encoded::scalars::{
    encode_scalar_blank_node, encode_scalar_literal, encode_scalar_named_node,
};
use arrow_rdf::encoded::{EncTerm, ENC_AS_NATIVE_BOOLEAN, ENC_EQ, ENC_QUAD_SCHEMA};
use arrow_rdf::{COL_OBJECT, COL_PREDICATE, COL_SUBJECT, TABLE_QUADS};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{not_impl_err, Column, DFSchema, DFSchemaRef, JoinType, ScalarValue};
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::logical_expr::{lit, Expr, LogicalPlan, LogicalPlanBuilder, LogicalTableSource};
use datafusion::prelude::col;
use oxrdf::{Variable, VariableRef};
use spargebra::algebra::{Expression, GraphPattern};
use spargebra::term::{GroundTerm, TermPattern, TriplePattern};
use std::collections::HashSet;
use std::sync::Arc;

pub struct GraphPatternRewriter<'a> {
    state: &'a SessionState,
}

impl<'a> GraphPatternRewriter<'a> {
    pub fn new(state: &'a SessionState) -> Self {
        Self { state }
    }

    pub fn rewrite(&self, pattern: &GraphPattern) -> DFResult<LogicalPlan> {
        let plan = self.rewrite_graph_pattern(pattern)?;
        Ok(decode_rdf_terms(plan.build()?)?)
    }

    fn rewrite_graph_pattern(&self, pattern: &GraphPattern) -> DFResult<LogicalPlanBuilder> {
        match pattern {
            GraphPattern::Bgp { patterns } => self.rewrite_bgp(patterns),
            GraphPattern::Project { inner, variables } => self.rewrite_project(inner, variables),
            GraphPattern::Filter { inner, expr } => self.rewrite_filter(inner, expr),
            GraphPattern::Extend {
                inner,
                expression,
                variable,
            } => self.rewrite_extend(inner, expression, variable),
            GraphPattern::Values {
                variables,
                bindings,
            } => self.rewrite_values(variables, bindings),
            pattern => not_impl_err!("{:?}", pattern),
        }
    }

    fn rewrite_bgp(&self, patterns: &Vec<TriplePattern>) -> DFResult<LogicalPlanBuilder> {
        patterns
            .iter()
            .map(|p| self.rewrite_triple_pattern(p))
            .reduce(|lhs, rhs| self.join_solutions(lhs?, rhs?))
            .unwrap_or_else(|| {
                Ok(LogicalPlanBuilder::scan(
                    TABLE_QUADS,
                    Arc::new(LogicalTableSource::new(ENC_QUAD_SCHEMA.clone())),
                    None,
                )?)
            })
    }

    fn rewrite_project(
        &self,
        inner: &GraphPattern,
        variables: &Vec<Variable>,
    ) -> DFResult<LogicalPlanBuilder> {
        self.rewrite_graph_pattern(inner)?
            .project(variables.iter().map(|v| col(v.as_str())))
    }

    fn rewrite_filter(
        &self,
        inner: &GraphPattern,
        expr: &Expression,
    ) -> DFResult<LogicalPlanBuilder> {
        let as_boolean = self.state.udf(ENC_AS_NATIVE_BOOLEAN.name())?;
        self.rewrite_graph_pattern(inner)?
            .filter(as_boolean.call(vec![self.rewrite_expr(expr)?]))
    }

    fn rewrite_extend(
        &self,
        inner: &GraphPattern,
        expression: &Expression,
        variable: &Variable,
    ) -> DFResult<LogicalPlanBuilder> {
        let inner = self.rewrite_graph_pattern(inner)?;

        let mut new_exprs: Vec<_> = inner
            .schema()
            .fields()
            .iter()
            .map(|f| Expr::Column(Column::from(f.name())))
            .collect();
        new_exprs.push(self.rewrite_expr(expression)?.alias(variable.as_str()));

        inner.project(new_exprs)
    }

    fn rewrite_values(
        &self,
        variables: &Vec<Variable>,
        bindings: &Vec<Vec<Option<GroundTerm>>>,
    ) -> DFResult<LogicalPlanBuilder> {
        if bindings.is_empty() {
            return Ok(LogicalPlanBuilder::empty(false));
        }

        let fields: Vec<_> = variables
            .iter()
            .map(|v| Field::new(v.as_str(), EncTerm::term_type(), true))
            .collect();
        let schema = DFSchemaRef::new(DFSchema::try_from(Schema::new(fields))?);

        let values = bindings
            .iter()
            .map(|solution| encode_solution(solution))
            .collect::<DFResult<Vec<_>>>()?;

        LogicalPlanBuilder::values_with_schema(values, &schema)
    }

    fn rewrite_triple_pattern(&self, pattern: &TriplePattern) -> DFResult<LogicalPlanBuilder> {
        let plan = LogicalPlanBuilder::scan(
            TABLE_QUADS,
            Arc::new(LogicalTableSource::new(ENC_QUAD_SCHEMA.clone())),
            None,
        )?;

        let (subject_filter, subject_projection) =
            pattern_to_filter_and_projections(&pattern.subject)?;
        let predicate_term_pattern = pattern.predicate.clone().into_term_pattern();
        let (predicate_filter, predicate_projection) =
            pattern_to_filter_and_projections(&predicate_term_pattern)?;
        let (object_filter, object_projection) =
            pattern_to_filter_and_projections(&pattern.object)?;

        let plan = self.apply_filter(plan, COL_SUBJECT, subject_filter)?;
        let plan = self.apply_filter(plan, COL_PREDICATE, predicate_filter)?;
        let plan = self.apply_filter(plan, COL_OBJECT, object_filter)?;

        let projections = [
            (COL_SUBJECT, subject_projection),
            (COL_PREDICATE, predicate_projection),
            (COL_OBJECT, object_projection),
        ]
        .into_iter()
        .filter_map(|(col_name, var)| {
            var.map(|new_col_name| col(col_name).alias(new_col_name.as_str()))
        });

        plan.project(projections)
    }

    fn apply_filter(
        &self,
        plan: LogicalPlanBuilder,
        col_name: &str,
        filter: Option<ScalarValue>,
    ) -> DFResult<LogicalPlanBuilder> {
        if filter.is_none() {
            return Ok(plan);
        }

        let rdf_eq = self.state.udf(ENC_EQ.name())?;
        let as_boolean = self.state.udf(ENC_EQ.name())?;
        plan.filter(as_boolean.call(vec![rdf_eq.call(vec![col(col_name), lit(filter.unwrap())])]))
    }

    fn join_solutions(
        &self,
        lhs: LogicalPlanBuilder,
        rhs: LogicalPlanBuilder,
    ) -> DFResult<LogicalPlanBuilder> {
        let rdf_eq = self.state.udf(ENC_EQ.name())?;
        let as_boolean = self.state.udf(ENC_EQ.name())?;
        let lhs = lhs.alias("lhs")?;
        let rhs = rhs.alias("rhs")?;
        let lhs_keys: HashSet<_> = lhs.schema().field_names().iter().cloned().collect();
        let rhs_keys: HashSet<_> = rhs.schema().field_names().iter().cloned().collect();
        let join_on_exprs = lhs_keys.intersection(&rhs_keys).map(|k| {
            as_boolean.call(vec![rdf_eq.call(vec![
                col(String::from("lhs.") + k),
                col(String::from("rhs.") + k),
            ])])
        });
        lhs.join_on(rhs.build()?, JoinType::Inner, join_on_exprs)
    }

    //
    // Expressions
    //

    fn rewrite_expr(&self, expression: &Expression) -> DFResult<Expr> {
        match expression {
            expr => not_impl_err!("{:?}", expr),
        }
    }
}

fn pattern_to_filter_and_projections(
    pattern: &TermPattern,
) -> DFResult<(Option<ScalarValue>, Option<VariableRef<'_>>)> {
    Ok(match pattern {
        TermPattern::NamedNode(nn) => (Some(encode_scalar_named_node(nn.as_ref())), None),
        TermPattern::BlankNode(bnode) => (Some(encode_scalar_blank_node(bnode.as_ref())), None),
        TermPattern::Literal(lit) => (Some(encode_scalar_literal(lit.as_ref())?), None),
        TermPattern::Variable(var) => (None, Some(var.as_ref())),
        TermPattern::Triple(_) => unimplemented!(),
    })
}

fn encode_solution(terms: &Vec<Option<GroundTerm>>) -> DFResult<Vec<Expr>> {
    terms
        .iter()
        .map(|t| {
            Ok(match t {
                Some(GroundTerm::NamedNode(nn)) => {
                    Expr::Literal(encode_scalar_named_node(nn.as_ref()))
                }
                Some(GroundTerm::Literal(lit)) => {
                    Expr::Literal(encode_scalar_literal(lit.as_ref())?)
                }
                None => Expr::Literal(ScalarValue::Null),
                _ => unimplemented!("encoding values"),
            })
        })
        .collect()
}
