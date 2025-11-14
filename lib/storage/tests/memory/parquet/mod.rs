use crate::memory::{create_function_registry, create_storage};
use crate::{example_quad, example_quad_in_graph};
use datafusion::config::ConfigOptions;
use insta::assert_binary_snapshot;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_model::Quad;
use rdf_fusion_storage::memory::{
    MemQuadPersistenceOptions, MemQuadStoragePersistence,
    ParquetMemQuadStoragePersistence,
};
use std::io::Write;
use std::sync::{Arc, RwLock};

/// A simple writer that is backed by a [`Vec<u8>`]. This implements interior mutability to allow
/// inspecting the written file.
#[derive(Clone)]
struct TestWriter {
    inner: Arc<RwLock<Vec<u8>>>,
}

impl TestWriter {
    /// Creates a new [`TestWriter`].
    pub fn new(inner: Vec<u8>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }
}

impl Write for TestWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write().expect("poison").write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_object_id_storage_export_parquet_as_plain_term_encoding() {
    let storage = create_storage();
    let registry = create_function_registry(storage.object_id_mapping().unwrap());

    storage
        .extend(vec![
            example_quad(),
            example_quad_in_graph("http://example.com/g"),
        ])
        .await
        .unwrap();

    let snapshot = storage.snapshot().await;
    let options = MemQuadPersistenceOptions::default()
        .with_encoding(QuadStorageEncoding::PlainTerm);

    let writer = TestWriter::new(Vec::<u8>::new());
    ParquetMemQuadStoragePersistence::new(
        registry,
        Arc::new(ConfigOptions::default()),
    )
    .export(writer.clone(), &snapshot, &options)
    .await
    .unwrap();

    assert_binary_snapshot!(
        "test_object_id_storage_export_parquet_as_plain_term_encoding.parquet",
        writer.inner.read().expect("poison").to_vec()
    );
}
