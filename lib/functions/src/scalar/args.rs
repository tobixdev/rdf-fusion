use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::logical_expr::ScalarFunctionArgs;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::{EncodingDatum, TermEncoding};

/// TODO
pub trait SparqlOpArgs {
    /// TODO
    fn try_from_args(args: ScalarFunctionArgs) -> DFResult<Self>
    where
        Self: Sized;
}

pub struct NullarySparqlOpArgs;

impl SparqlOpArgs for NullarySparqlOpArgs {
    fn try_from_args(args: ScalarFunctionArgs) -> DFResult<Self> {
        if args.args.len() != 0 {
            return exec_err!("Expected 0 arguments, got {}", args.args.len());
        }
        Ok(Self)
    }
}

pub struct UnarySparqlOpArgs<TEncoding: TermEncoding>(pub EncodingDatum<TEncoding>);

impl<TEncoding: TermEncoding> SparqlOpArgs for UnarySparqlOpArgs<TEncoding> {
    fn try_from_args(args: ScalarFunctionArgs) -> DFResult<Self> {
        let args = args
            .args
            .into_iter()
            .map(|cv| TEncoding::try_new_datum(cv, args.number_rows))
            .collect::<DFResult<Vec<_>>>()?;

        let len = args.len();
        let [arg0] = TryInto::<[EncodingDatum<TEncoding>; 1]>::try_into(args)
            .map_err(|_| exec_datafusion_err!("Expected 1 argument, got {}", len))?;

        Ok(Self(arg0))
    }
}
