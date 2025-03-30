use crate::encoded::dispatch::borrow_value;
use crate::encoded::write_enc_term::WriteEncTerm;
use crate::encoded::FromEncodedTerm;
use crate::DFResult;
use datafusion::logical_expr::ColumnarValue;
use functions_scalar::ScalarNAryRdfOp;

pub fn dispatch_n_ary<'data, TUdf>(
    udf: &TUdf,
    args: &'data [ColumnarValue],
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: ScalarNAryRdfOp,
    TUdf::Args<'data>: FromEncodedTerm<'data>,
    TUdf::Result<'data>: WriteEncTerm,
{
    let results = (0..number_of_rows).into_iter().map(|i| {
        let args = args
            .iter()
            .map(|a| borrow_value::<TUdf::Args<'data>>(a, i))
            .collect::<Vec<_>>();

        if args.iter().all(|arg| arg.is_ok()) {
            let args = args.into_iter().map(|arg| arg.unwrap()).collect::<Vec<_>>();
            udf.evaluate(args.as_slice())
        } else {
            udf.evaluate_error(args.as_slice())
        }
    });
    let result = TUdf::Result::iter_into_array(results)?;
    Ok(ColumnarValue::Array(result))
}
