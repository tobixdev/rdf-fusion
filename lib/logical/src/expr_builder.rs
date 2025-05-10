use crate::DFResult;
use datafusion::common::{plan_err, DFSchemaRef};
use datafusion::logical_expr::{lit, Expr, ExprSchemable};
use graphfusion_encoding::plain_term_encoding::PlainTermEncoding;
use graphfusion_encoding::value_encoding::TypedValueEncoding;
use graphfusion_encoding::{EncodingName, EncodingScalar, TermEncoding};
use graphfusion_functions::registry::GraphFusionBuiltinRegistryRef;
use graphfusion_model::TermRef;
use std::collections::HashMap;
use graphfusion_functions::builtin::BuiltinName;

/// TODO: Explain why
#[derive(Debug)]
pub struct GraphFusionExprBuilder {
    /// The schema of the input data. Necessary for inferring the encodings of RDF terms.
    schema: DFSchemaRef,
    /// Provides access to the builtin functions.
    registry: GraphFusionBuiltinRegistryRef,
}

impl GraphFusionExprBuilder {
    /// Creates a new [GraphFusionExprBuilder] with access to the provided `registry`.
    pub fn new(schema: DFSchemaRef, registry: GraphFusionBuiltinRegistryRef) -> Self {
        Self { schema, registry }
    }

    /// Returns the schema of the input data.
    pub fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    /// TODO
    pub fn effective_boolean_value(&self, expr: Expr) -> DFResult<Expr> {
        let udf = self
            .registry
            .scalar_factory(BuiltinName::EffectiveBooleanValue)
            .create_with_args(HashMap::new())?;
        Ok(udf.call(vec![expr]))
    }

    /// TODO
    pub fn filter_by_scalar(&self, expr: Expr, scalar: TermRef<'_>) -> DFResult<Expr> {
        let encoding_name = self.encoding(&expr)?;
        let literal = match encoding_name {
            EncodingName::PlainTerm => {
                PlainTermEncoding::encode_scalar(scalar)?.into_scalar_value()
            }
            EncodingName::TypedValue => {
                TypedValueEncoding::encode_scalar(scalar)?.into_scalar_value()
            }
            EncodingName::Sortable => {
                return plan_err!("Filtering not supported for Sortable encoding.")
            }
        };
        self.same_term(expr, lit(literal))
    }

    /// TODO
    pub fn same_term(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let effective_boolean_value = self
            .registry
            .scalar_factory(BuiltinName::EffectiveBooleanValue)
            .create_with_args(HashMap::new())?;
        let same_term = self
            .registry
            .scalar_factory(BuiltinName::SameTerm)
            .create_with_args(HashMap::new())?;
        Ok(effective_boolean_value.call(vec![same_term.call(vec![lhs, rhs])]))
    }

    /// Tries to obtain the encoding from a given expression.
    fn encoding(&self, expr: &Expr) -> DFResult<EncodingName> {
        let (data_type, _) = expr.data_type_and_nullable(&self.schema)?;

        if data_type == PlainTermEncoding::data_type() {
            return Ok(PlainTermEncoding::name());
        }

        if data_type == TypedValueEncoding::data_type() {
            return Ok(TypedValueEncoding::name());
        }

        plan_err!(
            "Expression does not have a valid RDF term encoding: {}",
            &data_type
        )
    }
}
