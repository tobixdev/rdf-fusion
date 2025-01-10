//! This module exposes the in-memory store from Oxigraph as table in GraphFusion.
pub(crate) mod encoded_term;
pub(crate) mod encoder;
pub(crate) mod hash;
pub(crate) mod oxigraph_mem_exec;
pub(crate) mod small_string;
pub(crate) mod store;
pub(crate) mod table_provider;
mod triple_store;

pub use triple_store::MemoryTripleStore;
