use crate::encoded::EncRdfTermBuilder;
use crate::DFResult;
use datafusion::logical_expr::ColumnarValue;

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
