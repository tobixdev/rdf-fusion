use crate::encoded::dispatch::borrow_value;
use crate::encoded::from_encoded_term::FromEncodedTerm;
use crate::encoded::write_enc_term::WriteEncTerm;
use crate::DFResult;
use datafusion::common::exec_err;
use datafusion::logical_expr::ColumnarValue;
use functions_scalar::ScalarQuaternaryRdfOp;

pub fn dispatch_quaternary<'data, TUdf>(
    udf: &TUdf,
    args: &'data [ColumnarValue],
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: ScalarQuaternaryRdfOp,
    TUdf::Arg0<'data>: FromEncodedTerm<'data>,
    TUdf::Arg1<'data>: FromEncodedTerm<'data>,
    TUdf::Arg2<'data>: FromEncodedTerm<'data>,
    TUdf::Arg3<'data>: FromEncodedTerm<'data>,
    TUdf::Result<'data>: WriteEncTerm,
{
    if args.len() != 4 {
        return exec_err!("Unexpected number of arguments.");
    }

    let results = (0..number_of_rows).into_iter().map(|i| {
        let arg0 = borrow_value::<TUdf::Arg0<'data>>(&args[0], i);
        let arg1 = borrow_value::<TUdf::Arg1<'data>>(&args[1], i);
        let arg2 = borrow_value::<TUdf::Arg2<'data>>(&args[2], i);
        let arg3 = borrow_value::<TUdf::Arg3<'data>>(&args[3], i);
        match (arg0, arg1, arg2, arg3) {
            (Ok(arg0), Ok(arg1), Ok(arg2), Ok(arg3)) => udf.evaluate(arg0, arg1, arg2, arg3),
            _ => udf.evaluate_error(),
        }
    });

    let result = TUdf::Result::iter_into_array(results)?;
    Ok(ColumnarValue::Array(result))
}
