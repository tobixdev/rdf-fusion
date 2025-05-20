use crate::{DFResult, RdfFusionExprBuilder};
use datafusion::common::{Column, DFSchema};
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{lit, Expr, ScalarUDF};
use rdf_fusion_encoding::plain_term::encoders::DefaultPlainTermEncoder;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::{EncodingName, EncodingScalar, TermEncoder, TermEncoding};
use rdf_fusion_functions::builtin::BuiltinName;
use rdf_fusion_functions::registry::RdfFusionFunctionRegistry;
use rdf_fusion_functions::FunctionName;
use rdf_fusion_model::{Term, TermRef, ThinError, VariableRef};
use std::collections::HashMap;
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
        Self { schema, registry }
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
    pub fn create_builder(&self, expr: Expr) -> RdfFusionExprBuilder<'root> {
        RdfFusionExprBuilder::new_from_root(*self, expr)
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
        Ok(self.create_builder(udf.call(vec![])))
    }

    /// TODO
    pub fn uuid(&self) -> DFResult<RdfFusionExprBuilder<'root>> {
        let udf = self.create_builtin_udf(BuiltinName::Uuid)?;
        Ok(self.create_builder(udf.call(vec![])))
    }

    /// TODO
    pub fn str_uuid(&self) -> DFResult<RdfFusionExprBuilder<'root>> {
        let udf = self.create_builtin_udf(BuiltinName::StrUuid)?;
        Ok(self.create_builder(udf.call(vec![])))
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
        Ok(self.create_builder(udf.call(vec![])))
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
        Ok(self.create_builder(expr))
    }

    /// TODO
    pub fn literal<'lit>(
        &self,
        term: impl Into<TermRef<'lit>>,
    ) -> DFResult<RdfFusionExprBuilder<'root>> {
        let scalar = DefaultPlainTermEncoder::encode_term(Ok(term.into()))?;
        Ok(self.create_builder(lit(scalar.into_scalar_value())))
    }

    /// TODO
    pub fn null_literal(&'root self) -> DFResult<RdfFusionExprBuilder<'root>> {
        let scalar = DefaultPlainTermEncoder::encode_term(ThinError::expected())?;
        Ok(self.create_builder(lit(scalar.into_scalar_value())))
    }

    //
    // Built-Ins
    //

    pub(crate) fn apply_builtin_udaf(
        &self,
        name: BuiltinName,
        arg: Expr,
        distinct: bool,
        args: HashMap<String, Term>,
    ) -> DFResult<RdfFusionExprBuilder<'root>> {
        let udaf = self
            .registry
            .create_udaf(FunctionName::Builtin(name), args)?;

        // Currently, UDAFs are only supported for typed values
        let arg = self
            .create_builder(arg)
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
        Ok(self.create_builder(expr))
    }

    /// TODO
    pub(crate) fn apply_builtin(
        &self,
        name: BuiltinName,
        further_args: Vec<Expr>,
    ) -> DFResult<RdfFusionExprBuilder<'root>> {
        self.apply_builtin_with_args(name, further_args, HashMap::new())
    }

    /// TODO
    pub(crate) fn apply_builtin_with_args(
        &self,
        name: BuiltinName,
        args: Vec<Expr>,
        udf_args: HashMap<String, Term>,
    ) -> DFResult<RdfFusionExprBuilder<'root>> {
        let udf = self.create_builtin_udf_with_args(name, udf_args)?;

        let target_encoding = TypedValueEncoding::name();
        let args = args
            .into_iter()
            .map(|expr| self.create_builder(expr))
            .map(|e| e.with_encoding(target_encoding)?.build())
            .collect::<DFResult<Vec<_>>>()?;
        Ok(self.create_builder(udf.call(args)))
    }

    //
    // Helper Functions
    //

    pub(crate) fn create_builtin_udf(&self, name: BuiltinName) -> DFResult<Arc<ScalarUDF>> {
        self.registry
            .create_udf(FunctionName::Builtin(name), HashMap::new())
    }

    pub(crate) fn create_builtin_udf_with_args(
        &self,
        name: BuiltinName,
        args: HashMap<String, Term>,
    ) -> DFResult<Arc<ScalarUDF>> {
        self.registry.create_udf(FunctionName::Builtin(name), args)
    }
}
