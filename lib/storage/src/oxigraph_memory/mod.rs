//! This module exposes the in-memory store from Oxigraph as table in RdfFusion.
mod encoded;
mod object_id;
mod object_id_mapping;
mod planner;
mod quad_storage;
mod quad_storage_stream;
mod small_string;
mod store;

pub use quad_storage::MemoryQuadStorage;
