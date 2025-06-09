//! This module exposes the in-memory store from Oxigraph as table in RdfFusion.
mod encoded_term;
mod encoder;
mod oxigraph_mem_exec;
mod planner;
mod quad_storage;
mod quad_storage_stream;
mod small_string;
mod store;
mod table_provider;

pub use quad_storage::MemoryQuadStorage;
