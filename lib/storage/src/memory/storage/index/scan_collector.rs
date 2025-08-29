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
    state: [Vec<u32>; 4],
}

impl ScanCollector {
    /// Creates a new [ScanCollector].
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            state: [
                Vec::default(),
                Vec::default(),
                Vec::default(),
                Vec::default(),
            ],
        }
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// TODO
    pub fn batch_full(&self) -> bool {
        self.state.iter().any(|v| v.len() == self.batch_size)
    }

    /// TODO
    pub fn num_results(&self, idx: usize) -> usize {
        self.state[idx].len()
    }

    /// TODO
    pub fn extend(&mut self, idx: usize, results: impl IntoIterator<Item = u32>) {
        let vec = &mut self.state[idx];
        vec.extend(results);
    }

    pub fn into_scan_batch(
        self,
        num_results: usize,
        vars: [Option<String>; 4],
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
        for (name, object_ids) in vars
            .into_iter()
            .zip(self.state.into_iter())
            .flat_map(|(name, object_ids)| name.map(|name| (name, object_ids)))
            .filter(|(_, object_ids)| !object_ids.is_empty())
        {
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
