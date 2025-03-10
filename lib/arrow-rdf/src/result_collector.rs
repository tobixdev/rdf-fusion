use crate::encoded::EncRdfTermBuilder;
use crate::DFResult;
use datafusion::arrow::array::BooleanBuilder;
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;

pub(crate) trait ResultCollector {
    fn new() -> Self;
    fn finish_columnar_value(self) -> DFResult<ColumnarValue>;
}

impl ResultCollector for EncRdfTermBuilder {
    fn new() -> Self {
        EncRdfTermBuilder::new()
    }

    fn finish_columnar_value(self) -> DFResult<ColumnarValue> {
        self.finish().map(ColumnarValue::Array)
    }
}

impl ResultCollector for BooleanBuilder {
    fn new() -> Self {
        BooleanBuilder::new()
    }

    fn finish_columnar_value(mut self) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Array(Arc::new(self.finish())))
    }
}
