use crate::builtin::BuiltinName;
use crate::scalar::dispatch::dispatch_ternary_typed_value;
use crate::scalar::sparql_op_impl::{create_typed_value_sparql_op_impl, SparqlOpImpl};
use crate::scalar::{ScalarSparqlOp, TernaryArgs};
use crate::FunctionName;
use datafusion::logical_expr::Volatility;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::TermEncoding;
use rdf_fusion_model::Boolean;

#[derive(Debug)]
pub struct IfSparqlOp;

impl Default for IfSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IfSparqlOp {
    const NAME: FunctionName = FunctionName::Builtin(BuiltinName::Bound);

    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarSparqlOp for IfSparqlOp {
    type Args<TEncoding: TermEncoding> = TernaryArgs<TEncoding>;

    fn name(&self) -> &FunctionName {
        &Self::NAME
    }

    fn volatility(&self) -> Volatility {
        Volatility::Immutable
    }

    fn typed_value_encoding_op(
        &self,
    ) -> Option<Box<dyn SparqlOpImpl<Self::Args<TypedValueEncoding>>>> {
        Some(create_typed_value_sparql_op_impl(
            |TernaryArgs(arg0, arg1, arg2)| {
                dispatch_ternary_typed_value(
                    &arg0,
                    &arg1,
                    &arg2,
                    |arg0, arg1, arg2| {
                        let test = Boolean::try_from(arg0)?;
                        if test.as_bool() {
                            Ok(arg1)
                        } else {
                            Ok(arg2)
                        }
                    },
                    |arg0, arg1, arg2| {
                        let test = Boolean::try_from(arg0?)?;
                        if test.as_bool() {
                            arg1
                        } else {
                            arg2
                        }
                    },
                )
            },
        ))
    }
}
