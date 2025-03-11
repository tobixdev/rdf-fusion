use crate::datatypes::RdfSimpleLiteral;
use crate::encoded::dispatch_unary::{dispatch_unary, EncScalarUnaryUdf};
use crate::encoded::{EncRdfTermBuilder, EncTerm};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::internal_err;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use oxrdf::BlankNode;
use rand::random;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct EncRand {
    signature: Signature,
}

impl EncRand {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Nullary, Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for EncRand {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_rand"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(EncTerm::term_type())
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        if args.len() != 0 {
            return internal_err!("Unexpected number of arguments");
        }

        let mut builder = EncRdfTermBuilder::new();
        for _ in 0..number_rows {
            builder.append_float64(random::<f64>().into())?;
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish()?)))
    }
}

#[derive(Debug)]
pub struct EncBNodeUnary {
    signature: Signature,
}

impl EncBNodeUnary {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![EncTerm::term_type()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarUnaryUdf for EncBNodeUnary {
    type Arg<'data> = RdfSimpleLiteral<'data>;
    type Collector = EncRdfTermBuilder;

    fn evaluate(&self, collector: &mut Self::Collector, value: Self::Arg<'_>) -> DFResult<()> {
        let Ok(result) = BlankNode::new(value.value) else {
            collector.append_null()?;
            return Ok(());
        };
        collector.append_blank_node(result.as_str())?;
        Ok(())
    }

    fn evaluate_error(&self, collector: &mut Self::Collector) -> DFResult<()> {
        collector.append_null()?;
        Ok(())
    }
}

impl ScalarUDFImpl for EncBNodeUnary {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_bnode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(EncTerm::term_type())
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        if args.len() != 1 {
            return internal_err!("Unexpected number of arguments");
        }
        dispatch_unary(self, args, number_rows)
    }
}
