use crate::memory::object_id::DEFAULT_GRAPH_ID;
use crate::memory::storage::index::{IndexConfiguration, IndexScanBatch};
use datafusion::arrow::array::UInt32Array;
use rdf_fusion_encoding::TermEncoding;
use std::collections::HashMap;
use std::sync::Arc;

/// TODO
pub struct ScanCollector {
    /// TODO
    batch_size: usize,
    /// TODO
    state: HashMap<String, Vec<u32>>,
}

impl ScanCollector {
    /// Creates a new [ScanCollector].
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            state: HashMap::new(),
        }
    }

    /// TODO
    pub fn batch_full(&self) -> bool {
        self.max_num_results() >= self.batch_size
    }

    /// TODO
    pub fn max_num_results(&self) -> usize {
        self.state.values().map(|v| v.len()).max().unwrap_or(0)
    }

    /// TODO
    pub fn num_results(&self, name: &str) -> usize {
        self.state.get(name).map(|v| v.len()).unwrap_or(0)
    }

    /// TODO
    pub fn extend(&mut self, name: &str, results: impl IntoIterator<Item = u32>) {
        if let Some(vec) = self.state.get_mut(name) {
            vec.extend(results);
        } else {
            let mut vec = Vec::with_capacity(self.batch_size);
            vec.extend(results);
            self.state.insert(name.to_owned(), vec);
        }
    }

    pub fn into_scan_batch(
        self,
        num_results: usize,
        configuration: &IndexConfiguration,
    ) -> IndexScanBatch {
        // This is the result when only an index lookup is performed.
        if num_results == 1 && self.state.is_empty() {
            return IndexScanBatch {
                num_results: 1,
                columns: HashMap::new(),
            };
        }

        let mut columns = HashMap::new();
        for (name, object_ids) in self.state.into_iter() {
            assert_eq!(
                object_ids.len(),
                num_results,
                "Unexpected number of collected elements in '{name}'"
            );

            let arrow_array = UInt32Array::from_iter(object_ids.into_iter().map(|v| {
                if v == DEFAULT_GRAPH_ID.0.as_u32() {
                    None
                } else {
                    Some(v)
                }
            }));
            let object_id_array = configuration
                .object_id_encoding
                .try_new_array(Arc::new(arrow_array))
                .expect("TODO");
            columns.insert(name, object_id_array);
        }

        IndexScanBatch {
            num_results,
            columns,
        }
    }
}
