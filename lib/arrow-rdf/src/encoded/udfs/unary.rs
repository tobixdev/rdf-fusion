use crate::encoded::udfs::unary_dispatch::EncScalarUnaryUdf;
use crate::encoded::EncRdfTermBuilder;
use crate::DFResult;

struct EncNot {}

impl EncScalarUnaryUdf for EncNot {
    type Collector = EncRdfTermBuilder;
    
    fn supports_boolean() -> bool {
        true
    }

    fn eval_boolean(collector: &mut Self::Collector, value: bool) -> DFResult<()> {
        Ok(collector.append_boolean(value)?)
    }

    fn eval_null(collector: &mut Self::Collector) -> DFResult<()> {
        Ok(collector.append_null()?)
    }
}
