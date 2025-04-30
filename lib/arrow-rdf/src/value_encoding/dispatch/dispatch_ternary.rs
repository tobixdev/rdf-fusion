use crate::value_encoding::dispatch::borrow_value;
use crate::value_encoding::from_encoded_term::FromEncodedTerm;
use crate::value_encoding::write_enc_term::WriteEncTerm;
use crate::DFResult;
use datafusion::common::exec_err;
use datafusion::logical_expr::ColumnarValue;
use functions_scalar::ScalarTernaryRdfOp;

pub fn dispatch_ternary<'data, TUdf>(
    udf: &TUdf,
    args: &'data [ColumnarValue],
    number_of_rows: usize,
) -> DFResult<ColumnarValue>
where
    TUdf: ScalarTernaryRdfOp,
    TUdf::Arg0<'data>: FromEncodedTerm<'data>,
    TUdf::Arg1<'data>: FromEncodedTerm<'data>,
    TUdf::Arg2<'data>: FromEncodedTerm<'data>,
    TUdf::Result<'data>: WriteEncTerm,
{
    if args.len() != 3 {
        return exec_err!("Unexpected number of arguments.");
    }

    #[allow(
        clippy::missing_asserts_for_indexing,
        reason = "Already checked and not performance critical"
    )]
    let results = (0..number_of_rows).map(|i| {
        let arg0 = borrow_value::<TUdf::Arg0<'data>>(&args[0], i);
        let arg1 = borrow_value::<TUdf::Arg1<'data>>(&args[1], i);
        let arg2 = borrow_value::<TUdf::Arg2<'data>>(&args[2], i);
        match (arg0, arg1, arg2) {
            (Ok(arg0), Ok(arg1), Ok(arg2)) => udf.evaluate(arg0, arg1, arg2),
            _ => udf.evaluate_error(arg0, arg1, arg2),
        }
    });
    let result = TUdf::Result::iter_into_array(results)?;
    Ok(ColumnarValue::Array(result))
}
