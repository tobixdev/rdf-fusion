use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::logical_expr::{ScalarFunctionArgs, TypeSignature};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::{EncodingDatum, TermEncoding};

/// TODO
pub trait SparqlOpArgs {
    /// TODO
    fn try_from_args(args: ScalarFunctionArgs) -> DFResult<Self>
    where
        Self: Sized;

    /// TODO
    fn type_signature() -> TypeSignature;
}

pub struct NullaryArgs {
    pub number_rows: usize,
}

impl SparqlOpArgs for NullaryArgs {
    fn try_from_args(args: ScalarFunctionArgs) -> DFResult<Self> {
        if args.args.len() != 0 {
            return exec_err!("Expected 0 arguments, got {}", args.args.len());
        }
        Ok(Self {
            number_rows: args.number_rows,
        })
    }

    fn type_signature() -> TypeSignature {
        TypeSignature::Nullary
    }
}

pub struct UnaryArgs<TEncoding: TermEncoding>(pub EncodingDatum<TEncoding>);

impl<TEncoding: TermEncoding> SparqlOpArgs for UnaryArgs<TEncoding> {
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

    fn type_signature() -> TypeSignature {
        TypeSignature::Uniform(1, vec![TEncoding::data_type()])
    }
}

pub enum NullaryOrUnaryArgs<TEncoding: TermEncoding> {
    Nullary(NullaryArgs),
    Unary(UnaryArgs<TEncoding>),
}

impl<TEncoding: TermEncoding> SparqlOpArgs for NullaryOrUnaryArgs<TEncoding> {
    fn try_from_args(args: ScalarFunctionArgs) -> DFResult<Self> {
        if args.args.len() == 0 {
            Ok(Self::Nullary(NullaryArgs::try_from_args(args)?))
        } else {
            Ok(Self::Unary(UnaryArgs::try_from_args(args)?))
        }
    }

    fn type_signature() -> TypeSignature {
        TypeSignature::OneOf(vec![
            NullaryArgs::type_signature(),
            UnaryArgs::<TEncoding>::type_signature(),
        ])
    }
}
