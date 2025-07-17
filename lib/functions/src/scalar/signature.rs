use crate::scalar::{NullaryOrUnaryArgs, NullaryArgs, SparqlOpArgs, UnaryArgs};
use datafusion::logical_expr::TypeSignature;
use rdf_fusion_encoding::TermEncoding;
use std::fmt::Debug;

/// TODO
pub trait SparqlOpSignature<TEncoding>: Debug + Send + Sync
where
    TEncoding: TermEncoding,
{
    /// TODO
    type Args: SparqlOpArgs;

    /// TODO
    fn type_signature(&self) -> TypeSignature;
}

/// TODO
#[derive(Debug)]
pub struct NullarySparqlOpSignature;

impl<TEncoding> SparqlOpSignature<TEncoding> for NullarySparqlOpSignature
where
    TEncoding: TermEncoding,
{
    type Args = NullaryArgs;

    fn type_signature(&self) -> TypeSignature {
        TypeSignature::Nullary
    }
}

/// TODO
#[derive(Debug)]
pub struct UnarySparqlOpSignature;

impl<TEncoding> SparqlOpSignature<TEncoding> for UnarySparqlOpSignature
where
    TEncoding: TermEncoding,
{
    type Args = UnaryArgs<TEncoding>;

    fn type_signature(&self) -> TypeSignature {
        TypeSignature::Uniform(1, vec![TEncoding::data_type()])
    }
}

/// TODO
#[derive(Debug)]
pub struct NullaryOrUnarySparqlOpSignature;

impl<TEncoding> SparqlOpSignature<TEncoding> for NullaryOrUnarySparqlOpSignature
where
    TEncoding: TermEncoding,
{
    type Args = NullaryOrUnaryArgs<TEncoding>;

    fn type_signature(&self) -> TypeSignature {
        TypeSignature::OneOf(vec![
            TypeSignature::Nullary,
            TypeSignature::Uniform(1, vec![TEncoding::data_type()]),
        ])
    }
}
