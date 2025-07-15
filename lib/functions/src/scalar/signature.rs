use crate::scalar::{NullarySparqlOpArgs, SparqlOpArgs, UnarySparqlOpArgs};
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
    type Args = NullarySparqlOpArgs;

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
    type Args = UnarySparqlOpArgs<TEncoding>;

    fn type_signature(&self) -> TypeSignature {
        TypeSignature::Uniform(1, vec![TEncoding::data_type()])
    }
}
