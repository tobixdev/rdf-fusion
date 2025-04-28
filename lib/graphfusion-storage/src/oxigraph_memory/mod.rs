//! This module exposes the in-memory store from Oxigraph as table in GraphFusion.
mod encoded_term;
mod encoder;
mod hash;
mod oxigraph_mem_exec;
mod small_string;
mod store;
mod table_provider;
mod quad_storage;

pub use quad_storage::MemoryQuadStorage;
