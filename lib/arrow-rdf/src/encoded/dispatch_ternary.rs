use crate::datatypes::RdfValue;
use crate::result_collector::ResultCollector;
use crate::{as_enc_term_array, DFResult};
use datafusion::arrow::array::Array;
use datafusion::common::exec_err;
use datafusion::logical_expr::ColumnarValue;

pub(crate) trait EncScalarTernaryUdf {
    type Arg0<'data1>: RdfValue<'data1>;
    type Arg1<'data2>: RdfValue<'data2>;
    type Arg2<'data3>: RdfValue<'data3>;
    type Collector: ResultCollector;

    fn evaluate(
        collector: &mut Self::Collector,
        arg0: &Self::Arg0<'_>,
        arg1: &Self::Arg1<'_>,
        arg2: &Self::Arg2<'_>,
    ) -> DFResult<()>;

    fn evaluate_error(collector: &mut Self::Collector) -> DFResult<()>;
}

pub fn dispatch_ternary<TUdf>(
    args: &[ColumnarValue],
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: EncScalarTernaryUdf,
{
    if args.len() != 3 {
        return exec_err!("Unexpected number of arguments.");
    }

    // TODO improve for scalars
    let arg0_arr = args[0].to_array(number_of_rows)?;
    let arg1_arr = args[1].to_array(number_of_rows)?;
    let arg2_arr = args[2].to_array(number_of_rows)?;

    let arg0_arr = as_enc_term_array(&arg0_arr)?;
    let arg1_arr = as_enc_term_array(&arg1_arr)?;
    let arg2_arr = as_enc_term_array(&arg2_arr)?;

    let mut collector = TUdf::Collector::new();
    for i in 0..number_of_rows {
        let arg0 = TUdf::Arg0::from_enc_array(arg0_arr, i);
        let arg1 = TUdf::Arg1::from_enc_array(arg1_arr, i);
        let arg2 = TUdf::Arg2::from_enc_array(arg2_arr, i);

        match (arg0, arg1, arg2) {
            (Ok(arg0), Ok(arg1), Ok(arg2)) => TUdf::evaluate(&mut collector, &arg0, &arg1, &arg2)?,
            _ => TUdf::evaluate_error(&mut collector)?,
        }
    }
    collector.finish_columnar_value()
}
