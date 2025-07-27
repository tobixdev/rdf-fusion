use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::logical_expr::{ScalarFunctionArgs, TypeSignature};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::{EncodingDatum, TermEncoding};

/// TODO
pub trait SparqlOpArgs<TEncoding: TermEncoding> {
    /// TODO
    fn try_from_args(encoding: &TEncoding, args: ScalarFunctionArgs) -> DFResult<Self>
    where
        Self: Sized;

    /// TODO
    fn type_signature(encoding: &TEncoding) -> TypeSignature;
}

pub struct NullaryArgs {
    pub number_rows: usize,
}

impl<TEncoding: TermEncoding> SparqlOpArgs<TEncoding> for NullaryArgs {
    fn try_from_args(_encoding: &TEncoding, args: ScalarFunctionArgs) -> DFResult<Self> {
        if !args.args.is_empty() {
            return exec_err!("Expected 0 arguments, got {}", args.args.len());
        }
        Ok(Self {
            number_rows: args.number_rows,
        })
    }

    fn type_signature(_encoding: &TEncoding) -> TypeSignature {
        TypeSignature::Nullary
    }
}

pub struct UnaryArgs<TEncoding: TermEncoding>(pub EncodingDatum<TEncoding>);

impl<TEncoding: TermEncoding> SparqlOpArgs<TEncoding> for UnaryArgs<TEncoding> {
    fn try_from_args(encoding: &TEncoding, args: ScalarFunctionArgs) -> DFResult<Self> {
        let args = args
            .args
            .into_iter()
            .map(|cv| encoding.try_new_datum(cv, args.number_rows))
            .collect::<DFResult<Vec<_>>>()?;

        let len = args.len();
        let [arg0] = TryInto::<[EncodingDatum<TEncoding>; 1]>::try_into(args)
            .map_err(|_| exec_datafusion_err!("Expected 1 argument, got {}", len))?;

        Ok(Self(arg0))
    }

    fn type_signature(encoding: &TEncoding) -> TypeSignature {
        TypeSignature::Uniform(1, vec![encoding.data_type()])
    }
}

pub struct BinaryArgs<TEncoding: TermEncoding>(
    pub EncodingDatum<TEncoding>,
    pub EncodingDatum<TEncoding>,
);

impl<TEncoding: TermEncoding> SparqlOpArgs<TEncoding> for BinaryArgs<TEncoding> {
    fn try_from_args(encoding: &TEncoding, args: ScalarFunctionArgs) -> DFResult<Self> {
        let args = args
            .args
            .into_iter()
            .map(|cv| encoding.try_new_datum(cv, args.number_rows))
            .collect::<DFResult<Vec<_>>>()?;

        let len = args.len();
        let [arg0, arg1] = TryInto::<[EncodingDatum<TEncoding>; 2]>::try_into(args)
            .map_err(|_| exec_datafusion_err!("Expected 2 argument, got {}", len))?;

        Ok(Self(arg0, arg1))
    }

    fn type_signature(encoding: &TEncoding) -> TypeSignature {
        TypeSignature::Uniform(2, vec![encoding.data_type()])
    }
}

pub struct TernaryArgs<TEncoding: TermEncoding>(
    pub EncodingDatum<TEncoding>,
    pub EncodingDatum<TEncoding>,
    pub EncodingDatum<TEncoding>,
);

impl<TEncoding: TermEncoding> SparqlOpArgs<TEncoding> for TernaryArgs<TEncoding> {
    fn try_from_args(encoding: &TEncoding, args: ScalarFunctionArgs) -> DFResult<Self> {
        let args = args
            .args
            .into_iter()
            .map(|cv| encoding.try_new_datum(cv, args.number_rows))
            .collect::<DFResult<Vec<_>>>()?;

        let len = args.len();
        let [arg0, arg1, arg2] = TryInto::<[EncodingDatum<TEncoding>; 3]>::try_into(args)
            .map_err(|_| exec_datafusion_err!("Expected 2 argument, got {}", len))?;

        Ok(Self(arg0, arg1, arg2))
    }

    fn type_signature(encoding: &TEncoding) -> TypeSignature {
        TypeSignature::Uniform(3, vec![encoding.data_type()])
    }
}

pub struct QuaternaryArgs<TEncoding: TermEncoding>(
    pub EncodingDatum<TEncoding>,
    pub EncodingDatum<TEncoding>,
    pub EncodingDatum<TEncoding>,
    pub EncodingDatum<TEncoding>,
);

impl<TEncoding: TermEncoding> SparqlOpArgs<TEncoding> for QuaternaryArgs<TEncoding> {
    fn try_from_args(encoding: &TEncoding, args: ScalarFunctionArgs) -> DFResult<Self> {
        let args = args
            .args
            .into_iter()
            .map(|cv| encoding.try_new_datum(cv, args.number_rows))
            .collect::<DFResult<Vec<_>>>()?;

        let len = args.len();
        let [arg0, arg1, arg2, arg3] =
            TryInto::<[EncodingDatum<TEncoding>; 4]>::try_into(args)
                .map_err(|_| exec_datafusion_err!("Expected 2 argument, got {}", len))?;

        Ok(Self(arg0, arg1, arg2, arg3))
    }

    fn type_signature(encoding: &TEncoding) -> TypeSignature {
        TypeSignature::Uniform(4, vec![encoding.data_type()])
    }
}

pub struct NAryArgs<TEncoding: TermEncoding>(
    pub Vec<EncodingDatum<TEncoding>>,
    pub usize,
);

impl<TEncoding: TermEncoding> SparqlOpArgs<TEncoding> for NAryArgs<TEncoding> {
    fn try_from_args(encoding: &TEncoding, args: ScalarFunctionArgs) -> DFResult<Self> {
        let number_rows = args.number_rows;
        let args = args
            .args
            .into_iter()
            .map(|cv| encoding.try_new_datum(cv, args.number_rows))
            .collect::<DFResult<Vec<_>>>()?;
        Ok(Self(args, number_rows))
    }

    fn type_signature(encoding: &TEncoding) -> TypeSignature {
        TypeSignature::OneOf(vec![
            TypeSignature::Nullary,
            TypeSignature::Variadic(vec![encoding.data_type()]),
        ])
    }
}

pub enum NullaryOrUnaryArgs<TEncoding: TermEncoding> {
    Nullary(NullaryArgs),
    Unary(UnaryArgs<TEncoding>),
}

impl<TEncoding: TermEncoding> SparqlOpArgs<TEncoding> for NullaryOrUnaryArgs<TEncoding> {
    fn try_from_args(encoding: &TEncoding, args: ScalarFunctionArgs) -> DFResult<Self> {
        if args.args.is_empty() {
            Ok(Self::Nullary(NullaryArgs::try_from_args(encoding, args)?))
        } else {
            Ok(Self::Unary(UnaryArgs::try_from_args(encoding, args)?))
        }
    }

    fn type_signature(encoding: &TEncoding) -> TypeSignature {
        TypeSignature::OneOf(vec![
            NullaryArgs::type_signature(encoding),
            UnaryArgs::<TEncoding>::type_signature(encoding),
        ])
    }
}

pub enum BinaryOrTernaryArgs<TEncoding: TermEncoding> {
    Binary(BinaryArgs<TEncoding>),
    Ternary(TernaryArgs<TEncoding>),
}

impl<TEncoding: TermEncoding> SparqlOpArgs<TEncoding> for BinaryOrTernaryArgs<TEncoding> {
    fn try_from_args(encoding: &TEncoding, args: ScalarFunctionArgs) -> DFResult<Self> {
        if args.args.len() == 2 {
            Ok(Self::Binary(BinaryArgs::try_from_args(encoding, args)?))
        } else {
            Ok(Self::Ternary(TernaryArgs::try_from_args(encoding, args)?))
        }
    }

    fn type_signature(encoding: &TEncoding) -> TypeSignature {
        TypeSignature::OneOf(vec![
            BinaryArgs::<TEncoding>::type_signature(encoding),
            TernaryArgs::<TEncoding>::type_signature(encoding),
        ])
    }
}

pub enum TernaryOrQuaternaryArgs<TEncoding: TermEncoding> {
    Ternary(TernaryArgs<TEncoding>),
    Quaternary(QuaternaryArgs<TEncoding>),
}

impl<TEncoding: TermEncoding> SparqlOpArgs<TEncoding>
    for TernaryOrQuaternaryArgs<TEncoding>
{
    fn try_from_args(encoding: &TEncoding, args: ScalarFunctionArgs) -> DFResult<Self> {
        if args.args.len() == 3 {
            Ok(Self::Ternary(TernaryArgs::try_from_args(encoding, args)?))
        } else {
            Ok(Self::Quaternary(QuaternaryArgs::try_from_args(
                encoding, args,
            )?))
        }
    }

    fn type_signature(encoding: &TEncoding) -> TypeSignature {
        TypeSignature::OneOf(vec![
            TernaryArgs::<TEncoding>::type_signature(encoding),
            QuaternaryArgs::<TEncoding>::type_signature(encoding),
        ])
    }
}
