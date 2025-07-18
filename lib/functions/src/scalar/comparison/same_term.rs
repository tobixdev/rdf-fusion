use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_binary_plain_term;
use crate::scalar::{BinaryArgs, ScalarSparqlOp};
use crate::FunctionName;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, Volatility};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::{EncodingName, TermEncoding};
use rdf_fusion_model::vocab::xsd;
use rdf_fusion_model::{LiteralRef, TermRef, ThinError};

/// Implementation of the SPARQL `SAME_TERM` operator.
#[derive(Debug)]
pub struct SameTermSparqlOp {}

impl Default for SameTermSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl SameTermSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::SameTerm);

    /// Creates a new [SameTermSparqlOp].
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for SameTermSparqlOp {
    type Args<TEncoding: TermEncoding> = BinaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn supported_encodings(&self) -> &[EncodingName] {
        &[EncodingName::PlainTerm]
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn return_type(&self, _input_encoding: Option<EncodingName>) -> DFResult<DataType> {
        Ok(PlainTermEncoding::data_type())
    }

    fn invoke_plain_term_encoding(
        &self,
        BinaryArgs(lhs, rhs): Self::Args<PlainTermEncoding>,
    ) -> DFResult<ColumnarValue> {
        dispatch_binary_plain_term(
            &lhs,
            &rhs,
            |lhs_value, rhs_value| {
                let value = if lhs_value == rhs_value {
                    "true"
                } else {
                    "false"
                };
                Ok(TermRef::Literal(LiteralRef::new_typed_literal(
                    value,
                    xsd::BOOLEAN,
                )))
            },
            |_, _| ThinError::expected(),
        )
    }
}
