use crate::{DFResult, RdfFusionExprBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_err, Column, DFSchema};
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{lit, Expr, ExprSchemable, ScalarUDF};
use rdf_fusion_encoding::plain_term::encoders::DefaultPlainTermEncoder;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, EncodingScalar, TermEncoder, TermEncoding};
use rdf_fusion_functions::builtin::BuiltinName;
use rdf_fusion_functions::registry::RdfFusionFunctionRegistry;
use rdf_fusion_functions::{FunctionName, RdfFusionFunctionArgs};
use rdf_fusion_model::{TermRef, ThinError, VariableRef};
use std::sync::Arc;

/// TODO
#[derive(Debug, Clone, Copy)]
pub struct RdfFusionExprBuilderRoot<'root> {
    /// Provides access to the builtin functions.
    registry: &'root dyn RdfFusionFunctionRegistry,
    /// The schema of the input data. Necessary for inferring the encodings of RDF terms.
    schema: &'root DFSchema,
}

impl<'root> RdfFusionExprBuilderRoot<'root> {
    /// Creates a new expression builder root.
    pub fn new(registry: &'root dyn RdfFusionFunctionRegistry, schema: &'root DFSchema) -> Self {
        Self { registry, schema }
    }

    /// Returns the schema of the input data.
    pub fn schema(&self) -> &DFSchema {
        self.schema
    }

    /// Returns a reference to the used function registry.
    pub fn registry(&self) -> &dyn RdfFusionFunctionRegistry {
        self.registry
    }

    /// TODO
    pub fn try_create_builder(&self, expr: Expr) -> DFResult<RdfFusionExprBuilder<'root>> {
        RdfFusionExprBuilder::try_new_from_root(*self, expr)
    }

    /// Creates a new expression that evaluates to the first argument that does not produce an
    /// error.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Coalesce](https://www.w3.org/TR/sparql11-query/#func-coalesce)
    pub fn coalesce(&self, args: Vec<Expr>) -> DFResult<RdfFusionExprBuilder<'root>> {
        self.apply_builtin(BuiltinName::Coalesce, args)
    }

    /// TODO
    pub fn bnode(&self) -> DFResult<RdfFusionExprBuilder<'root>> {
        let udf = self.create_builtin_udf(BuiltinName::BNode)?;
        self.try_create_builder(udf.call(vec![]))
    }

    /// TODO
    pub fn uuid(&self) -> DFResult<RdfFusionExprBuilder<'root>> {
        let udf = self.create_builtin_udf(BuiltinName::Uuid)?;
        self.try_create_builder(udf.call(vec![]))
    }

    /// TODO
    pub fn str_uuid(&self) -> DFResult<RdfFusionExprBuilder<'root>> {
        let udf = self.create_builtin_udf(BuiltinName::StrUuid)?;
        self.try_create_builder(udf.call(vec![]))
    }

    /// TODO
    pub fn concat(&self, args: Vec<Expr>) -> DFResult<RdfFusionExprBuilder<'root>> {
        self.apply_builtin(BuiltinName::Concat, args)
    }

    /// Creates an expression that computes a pseudo-random value between 0 and 1.
    ///
    /// The data type of the value is `xsd:double`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Rand](https://www.w3.org/TR/sparql11-query/#idp2130040)
    pub fn rand(&self) -> DFResult<RdfFusionExprBuilder<'root>> {
        let udf = self.create_builtin_udf(BuiltinName::Rand)?;
        self.try_create_builder(udf.call(vec![]))
    }

    /// TODO
    pub fn variable(&self, variable: VariableRef<'_>) -> DFResult<RdfFusionExprBuilder<'root>> {
        let column = Column::new_unqualified(variable.as_str());
        let expr = if self.schema.has_column(&column) {
            Expr::from(column)
        } else {
            let null = DefaultPlainTermEncoder::encode_term(ThinError::expected())?;
            lit(null.into_scalar_value())
        };

        self.try_create_builder(expr)
    }

    /// TODO
    pub fn literal<'lit>(
        &self,
        term: impl Into<TermRef<'lit>>,
    ) -> DFResult<RdfFusionExprBuilder<'root>> {
        let scalar = DefaultPlainTermEncoder::encode_term(Ok(term.into()))?;
        self.try_create_builder(lit(scalar.into_scalar_value()))
    }

    /// TODO
    pub fn null_literal(&'root self) -> DFResult<RdfFusionExprBuilder<'root>> {
        let scalar = DefaultPlainTermEncoder::encode_term(ThinError::expected())?;
        self.try_create_builder(lit(scalar.into_scalar_value()))
    }

    //
    // Native to Term
    //

    /// TODO
    pub fn native_int64_as_term(&self, expr: Expr) -> DFResult<RdfFusionExprBuilder<'root>> {
        let (data_type, _) = expr.data_type_and_nullable(self.schema)?;
        if data_type != DataType::Int64 {
            return plan_err!(
                "Expected Int64 argument for {}, got {}",
                BuiltinName::NativeInt64AsTerm,
                data_type
            );
        }

        let udf = self.create_builtin_udf(BuiltinName::NativeInt64AsTerm)?;
        self.try_create_builder(udf.call(vec![expr]))
    }

    /// TODO
    pub fn native_boolean_as_term(&self, expr: Expr) -> DFResult<RdfFusionExprBuilder<'root>> {
        let (data_type, _) = expr.data_type_and_nullable(self.schema)?;
        if data_type != DataType::Boolean {
            return plan_err!(
                "Expected boolean arguments for {}, got {}",
                BuiltinName::NativeBooleanAsTerm,
                data_type
            );
        }

        let udf = self.create_builtin_udf(BuiltinName::NativeBooleanAsTerm)?;
        self.try_create_builder(udf.call(vec![expr]))
    }

    //
    // Logical
    //

    /// TODO
    pub fn sparql_and(&self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let (lhs_data_type, _) = lhs.data_type_and_nullable(self.schema)?;
        let (rhs_data_type, _) = rhs.data_type_and_nullable(self.schema)?;
        if lhs_data_type != DataType::Boolean || rhs_data_type != DataType::Boolean {
            return plan_err!(
                "Expected boolean arguments for {}, got {} and {}",
                BuiltinName::And,
                lhs_data_type,
                rhs_data_type
            );
        }

        let udf = self.create_builtin_udf(BuiltinName::And)?;
        Ok(udf.call(vec![lhs, rhs]))
    }

    /// TODO
    pub fn sparql_or(self, lhs: Expr, rhs: Expr) -> DFResult<Expr> {
        let (lhs_data_type, _) = lhs.data_type_and_nullable(self.schema)?;
        let (rhs_data_type, _) = rhs.data_type_and_nullable(self.schema)?;
        if lhs_data_type != DataType::Boolean || rhs_data_type != DataType::Boolean {
            return plan_err!(
                "Expected boolean arguments for {}, got {} and {}",
                BuiltinName::Or,
                lhs_data_type,
                rhs_data_type
            );
        }

        let udf = self.create_builtin_udf(BuiltinName::Or)?;
        Ok(udf.call(vec![lhs, rhs]))
    }

    //
    // Built-Ins
    //

    pub(crate) fn apply_builtin_udaf(
        &self,
        name: BuiltinName,
        arg: Expr,
        distinct: bool,
        args: RdfFusionFunctionArgs,
    ) -> DFResult<RdfFusionExprBuilder<'root>> {
        let udaf = self
            .registry
            .create_udaf(FunctionName::Builtin(name), args)?;

        // Currently, UDAFs are only supported for typed values
        let arg = self
            .try_create_builder(arg)?
            .with_encoding(EncodingName::TypedValue)?
            .build()?;
        let expr = Expr::AggregateFunction(AggregateFunction::new_udf(
            udaf,
            vec![arg],
            distinct,
            None,
            None,
            None,
        ));

        self.try_create_builder(expr)
    }

    /// TODO
    pub(crate) fn apply_builtin(
        &self,
        name: BuiltinName,
        further_args: Vec<Expr>,
    ) -> DFResult<RdfFusionExprBuilder<'root>> {
        self.apply_builtin_with_args(name, further_args, RdfFusionFunctionArgs::empty())
    }

    /// TODO
    pub(crate) fn apply_builtin_with_args(
        &self,
        name: BuiltinName,
        args: Vec<Expr>,
        udf_args: RdfFusionFunctionArgs,
    ) -> DFResult<RdfFusionExprBuilder<'root>> {
        let udf = self.create_builtin_udf_with_args(name, udf_args)?;

        let target_encoding = TypedValueEncoding::name();
        let args = args
            .into_iter()
            .map(|expr| {
                self.try_create_builder(expr)?
                    .with_encoding(target_encoding)?
                    .build()
            })
            .collect::<DFResult<Vec<_>>>()?;

        self.try_create_builder(udf.call(args))
    }

    //
    // Helper Functions
    //

    pub(crate) fn create_builtin_udf(&self, name: BuiltinName) -> DFResult<Arc<ScalarUDF>> {
        self.registry
            .create_udf(FunctionName::Builtin(name), RdfFusionFunctionArgs::empty())
    }

    pub(crate) fn create_builtin_udf_with_args(
        &self,
        name: BuiltinName,
        args: RdfFusionFunctionArgs,
    ) -> DFResult<Arc<ScalarUDF>> {
        self.registry.create_udf(FunctionName::Builtin(name), args)
    }
}
