use crate::RdfFusionExprBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Column, DFSchema, exec_datafusion_err, plan_err};
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{Expr, ExprSchemable, ScalarUDF, lit};
use rdf_fusion_api::RdfFusionContextView;
use rdf_fusion_api::functions::{
    BuiltinName, FunctionName, RdfFusionFunctionArgs, RdfFusionFunctionRegistry,
};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::plain_term::encoders::DefaultPlainTermEncoder;
use rdf_fusion_encoding::{
    EncodingName, EncodingScalar, RdfFusionEncodings, TermEncoder,
};
use rdf_fusion_model::{TermRef, ThinError, VariableRef};
use std::sync::Arc;

/// An expression builder for creating SPARQL expressions.
///
/// This is the builder context, which can be used to create expression builders. Each builder
/// context has an associated schema. This schema is used for, for example, inferring the type of
/// built expressions and is therefore crucial.
///
/// Furthermore, the context holds a reference to an [RdfFusionFunctionRegistry] that is used to
/// resolve the registered built-ins and user-defined functions.
#[derive(Debug, Clone, Copy)]
pub struct RdfFusionExprBuilderContext<'context> {
    /// Provides access to the RDF Fusion configuration.
    rdf_fusion_context: &'context RdfFusionContextView,
    /// The schema of the input data. Necessary for inferring the encodings of RDF terms.
    schema: &'context DFSchema,
}

impl<'context> RdfFusionExprBuilderContext<'context> {
    /// Creates a new expression builder context.
    pub fn new(
        rdf_fusion_context: &'context RdfFusionContextView,
        schema: &'context DFSchema,
    ) -> Self {
        Self {
            rdf_fusion_context,
            schema,
        }
    }

    /// Returns the schema of the input data.
    pub fn schema(&self) -> &DFSchema {
        self.schema
    }

    /// Returns a reference to the used function registry.
    pub fn registry(&self) -> &dyn RdfFusionFunctionRegistry {
        self.rdf_fusion_context.functions().as_ref()
    }

    /// Returns a reference to the used object id encoding.
    pub fn encodings(&self) -> &'context RdfFusionEncodings {
        self.rdf_fusion_context.encodings()
    }

    /// Creates a new [RdfFusionExprBuilder] from an existing [Expr].
    pub fn try_create_builder(
        &self,
        expr: Expr,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        RdfFusionExprBuilder::try_new_from_context(*self, expr)
    }

    /// Creates a new expression that evaluates to the first argument that does not produce an
    /// error.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Coalesce](https://www.w3.org/TR/sparql11-query/#func-coalesce)
    pub fn coalesce(&self, args: Vec<Expr>) -> DFResult<RdfFusionExprBuilder<'context>> {
        self.apply_builtin(BuiltinName::Coalesce, args)
    }

    /// Creates an expression that generates a new blank node.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - BNODE](https://www.w3.org/TR/sparql11-query/#func-bnode)
    pub fn bnode(&self) -> DFResult<RdfFusionExprBuilder<'context>> {
        let udf = self.create_builtin_udf(BuiltinName::BNode)?;
        self.try_create_builder(udf.call(vec![]))
    }

    /// Creates an expression that computes a fresh IRI from the `urn:uuid:` scheme.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - UUID](https://www.w3.org/TR/sparql11-query/#func-uuid)
    pub fn uuid(&self) -> DFResult<RdfFusionExprBuilder<'context>> {
        let udf = self.create_builtin_udf(BuiltinName::Uuid)?;
        self.try_create_builder(udf.call(vec![]))
    }

    /// Creates an expression that computes a string literal representation of a UUID.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - STRUUID](https://www.w3.org/TR/sparql11-query/#func-struuid)
    pub fn str_uuid(&self) -> DFResult<RdfFusionExprBuilder<'context>> {
        let udf = self.create_builtin_udf(BuiltinName::StrUuid)?;
        self.try_create_builder(udf.call(vec![]))
    }

    /// Creates an expression that concatenates the string representations of its arguments.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - CONCAT](https://www.w3.org/TR/sparql11-query/#func-concat)
    pub fn concat(&self, args: Vec<Expr>) -> DFResult<RdfFusionExprBuilder<'context>> {
        self.apply_builtin(BuiltinName::Concat, args)
    }

    /// Creates an expression that computes a pseudo-random value between 0 and 1.
    ///
    /// The data type of the value is `xsd:double`.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Rand](https://www.w3.org/TR/sparql11-query/#idp2130040)
    pub fn rand(&self) -> DFResult<RdfFusionExprBuilder<'context>> {
        let udf = self.create_builtin_udf(BuiltinName::Rand)?;
        self.try_create_builder(udf.call(vec![]))
    }

    /// Creates an expression from a SPARQL variable.
    ///
    /// If the variable is not bound in the current context (i.e., not in the schema),
    /// the expression evaluates to a null literal.
    pub fn variable(
        &self,
        variable: VariableRef<'_>,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        let column = Column::new_unqualified(variable.as_str());
        let expr = if self.schema.has_column(&column) {
            Expr::from(column)
        } else {
            let null = DefaultPlainTermEncoder::encode_term(ThinError::expected())?;
            lit(null.into_scalar_value())
        };

        self.try_create_builder(expr)
    }

    /// Creates a literal expression from an RDF term.
    ///
    /// The term is encoded using the [PlainTermEncoding](rdf_fusion_encoding::plain_term::PlainTermEncoding).
    pub fn literal<'lit>(
        &self,
        term: impl Into<TermRef<'lit>>,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        let scalar = DefaultPlainTermEncoder::encode_term(Ok(term.into()))?;
        self.try_create_builder(lit(scalar.into_scalar_value()))
    }

    /// Creates a null literal expression.
    ///
    /// This is used to represent unbound variables or errors.
    pub fn null_literal(&'context self) -> DFResult<RdfFusionExprBuilder<'context>> {
        let scalar = DefaultPlainTermEncoder::encode_term(ThinError::expected())?;
        self.try_create_builder(lit(scalar.into_scalar_value()))
    }

    //
    // Native to Term
    //

    /// Creates an expression that converts a native `i64` value into an RDF term.
    ///
    /// This is done by encoding the literal and calling [BuiltinName::NativeInt64AsTerm].
    pub fn native_int64_as_term(
        &self,
        expr: Expr,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
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

    /// Creates an expression that converts a native boolean value into an RDF term.
    ///
    /// This is done by encoding the literal and calling [BuiltinName::NativeBooleanAsTerm].
    pub fn native_boolean_as_term(
        &self,
        expr: Expr,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
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

    /// Creates a SPARQL logical AND expression.
    ///
    /// This differs from a standard AND by its error treatment.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Logical-and](https://www.w3.org/TR/sparql11-query/#func-logical-and)
    /// - [SPARQL 1.1 - Filter Evaluation](https://www.w3.org/TR/sparql11-query/#evaluation)
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

    /// Creates a SPARQL logical OR expression.
    ///
    /// This differs from a standard OR by its error treatment.
    ///
    /// # Relevant Resources
    /// - [SPARQL 1.1 - Logical-or](https://www.w3.org/TR/sparql11-query/#func-logical-or)
    /// - [SPARQL 1.1 - Filter Evaluation](https://www.w3.org/TR/sparql11-query/#evaluation)
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

    /// Applies a built-in SPARQL aggregate function to a list of arguments.
    pub(crate) fn apply_builtin_udaf(
        &self,
        name: BuiltinName,
        arg: Expr,
        distinct: bool,
        args: RdfFusionFunctionArgs,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        let udaf = self
            .registry()
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

    /// Applies a built-in SPARQL function to a list of arguments.
    pub(crate) fn apply_builtin(
        &self,
        name: BuiltinName,
        further_args: Vec<Expr>,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        self.apply_builtin_with_args(name, further_args, RdfFusionFunctionArgs::empty())
    }

    /// Applies a built-in SPARQL function to a list of arguments, with additional arguments
    /// for the user-defined function itself.
    ///
    /// This can be used to support functions like `IRI` that requires a base IRI.
    pub(crate) fn apply_builtin_with_args(
        &self,
        name: BuiltinName,
        args: Vec<Expr>,
        udf_args: RdfFusionFunctionArgs,
    ) -> DFResult<RdfFusionExprBuilder<'context>> {
        let expr = self.apply_builtin_with_args_no_builder(name, args, udf_args)?;
        self.try_create_builder(expr)
    }

    /// Similar to [Self::apply_builtin_with_args] but does not wrap the resulting expression
    /// in a builder. Therefore, this method can be used to create expressions that do not evaluate
    /// to an RDF term.
    pub(crate) fn apply_builtin_with_args_no_builder(
        &self,
        name: BuiltinName,
        args: Vec<Expr>,
        udf_args: RdfFusionFunctionArgs,
    ) -> DFResult<Expr> {
        let udf = self.create_builtin_udf_with_args(name, udf_args)?;
        let supported_encodings = self
            .registry()
            .supported_encodings(FunctionName::Builtin(name))?;

        if supported_encodings.is_empty() {
            return plan_err!("No supported encodings for builtin '{}'", name);
        }

        let encodings = self.get_encodings(&args)?;
        let input_encoding = decide_input_encoding(&supported_encodings, &encodings)?;

        let args = args
            .into_iter()
            .map(|expr| {
                self.try_create_builder(expr)?
                    .with_encoding(input_encoding)?
                    .build()
            })
            .collect::<DFResult<Vec<_>>>()?;

        Ok(udf.call(args))
    }

    //
    // Helper Functions
    //

    pub(crate) fn create_builtin_udf(
        &self,
        name: BuiltinName,
    ) -> DFResult<Arc<ScalarUDF>> {
        self.registry()
            .create_udf(FunctionName::Builtin(name), RdfFusionFunctionArgs::empty())
    }

    pub(crate) fn create_builtin_udf_with_args(
        &self,
        name: BuiltinName,
        args: RdfFusionFunctionArgs,
    ) -> DFResult<Arc<ScalarUDF>> {
        self.registry()
            .create_udf(FunctionName::Builtin(name), args)
    }

    /// TODO
    pub(crate) fn get_encodings(&self, args: &[Expr]) -> DFResult<Vec<EncodingName>> {
        args.iter()
            .map(|e| {
                let (data_type, _) = e.data_type_and_nullable(self.schema)?;
                Ok(data_type)
            })
            .map(|r| {
                r.and_then(|dt| {
                    self.encodings().try_get_encoding_name(&dt).ok_or(
                        exec_datafusion_err!("Data type is not an RDF term '{}'", dt),
                    )
                })
            })
            .collect::<DFResult<Vec<_>>>()
    }
}

fn decide_input_encoding(
    supported_encodings: &[EncodingName],
    actual_encodings: &[EncodingName],
) -> DFResult<EncodingName> {
    if supported_encodings.is_empty() {
        return plan_err!("No supported encodings");
    }

    // If all arguments have a supported encoding we choose this one.
    for supported_encoding in supported_encodings {
        if actual_encodings.iter().all(|e| e == supported_encoding) {
            return Ok(*supported_encoding);
        }
    }

    // Otherwise we currently return the first encoding.
    Ok(supported_encodings[0])
}
