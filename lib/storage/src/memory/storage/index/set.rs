use crate::memory::encoding::EncodedQuad;
use crate::memory::object_id::{EncodedGraphObjectId, EncodedObjectId};
use crate::memory::storage::index::quad_index::MemQuadIndex;
use crate::memory::storage::index::{
    IndexComponents, IndexConfiguration, IndexRefInSet, IndexScanInstructions,
    IndexedQuad, MemQuadIndexScanIterator,
};
use datafusion::arrow::array::{Array, RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use rdf_fusion_model::StorageError;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::OwnedRwLockReadGuard;

/// Represents a set of multiple indexes, each of which indexes a different ordering of the
/// triple component (e.g., SPO, POS). This is necessary as different triple patterns require
/// different index structures.
///
/// For example, the pattern `<S> <P> ?o` can be best served by having an SPO index. The scan would
/// then look up `<S>`, traverse into the next level looking up `<P>`, and lastly scanning the
/// entries and binding them to `?o`. However, the triple pattern `?s <P> <O>` cannot be efficiently
/// evaluated with an SPO index. For this pattern, the query engine should use an POS or OPS index.
///
/// The [IndexSet] allows managing multiple such indices.
#[derive(Debug)]
pub struct IndexSet {
    named_graphs: HashSet<EncodedObjectId>,
    indexes: Vec<MemQuadIndex>,
}

impl IndexSet {
    /// Creates a new [IndexSet] with the given `object_id_encoding` and `batch_size`.
    pub fn new(
        object_id_encoding: ObjectIdEncoding,
        batch_size: usize,
        components: &[IndexComponents],
    ) -> Self {
        let indexes = components
            .iter()
            .map(|components| {
                MemQuadIndex::new(IndexConfiguration {
                    object_id_encoding: object_id_encoding.clone(),
                    batch_size,
                    components: *components,
                })
            })
            .collect();

        Self {
            named_graphs: HashSet::new(),
            indexes,
        }
    }

    /// Finds an index with the given `configuration`.
    pub fn find_index(
        &self,
        configuration: &IndexConfiguration,
    ) -> Option<&MemQuadIndex> {
        self.indexes
            .iter()
            .find(|index| index.configuration() == configuration)
    }

    /// Chooses the index for scanning the given `pattern`.
    ///
    /// This returns an [IndexConfiguration] that identifies the chosen index. Use
    /// [MemQuadIndexSetScanIterator] for executing the scan operation.
    pub fn choose_index(&self, pattern: &IndexScanInstructions) -> IndexConfiguration {
        self.indexes
            .iter()
            .rev() // Prefer SPO (max by uses the last on equality)
            .max_by(|lhs, rhs| {
                let lhs_pattern =
                    reorder_pattern(pattern, &lhs.configuration().components);
                let rhs_pattern =
                    reorder_pattern(pattern, &rhs.configuration().components);

                let lhs_score = lhs.compute_scan_score(&lhs_pattern);
                let rhs_score = rhs.compute_scan_score(&rhs_pattern);

                lhs_score.cmp(&rhs_score)
            })
            .expect("At least one index must be available")
            .configuration()
            .clone()
    }

    pub fn len(&self) -> usize {
        self.any_index().len()
    }

    pub fn insert(&mut self, quads: &[EncodedQuad]) -> Result<usize, StorageError> {
        let mut count = 0;
        for index in self.indexes.iter_mut() {
            let components = index.configuration().components;
            let quads = quads
                .iter()
                .map(|q| IndexedQuad([q.graph_name.0, q.subject, q.predicate, q.object]))
                .map(|q| reorder_quad(&q, &components));
            count = index.insert(quads);
        }

        for quad in quads.iter().filter(|q| !q.graph_name.is_default_graph()) {
            self.named_graphs.insert(quad.graph_name.0);
        }

        Ok(count)
    }

    pub fn remove(&mut self, quads: &[EncodedQuad]) -> usize {
        let mut count = 0;
        for index in self.indexes.iter_mut() {
            let components = index.configuration().components;
            let quads = quads
                .iter()
                .map(|q| IndexedQuad([q.graph_name.0, q.subject, q.predicate, q.object]))
                .map(|q| reorder_quad(&q, &components));
            count = index.remove(quads);
        }
        count
    }

    pub fn insert_named_graph(&mut self, graph_name: EncodedObjectId) -> bool {
        self.named_graphs.insert(graph_name)
    }

    pub fn named_graphs(&self) -> Vec<EncodedObjectId> {
        self.named_graphs.iter().copied().collect()
    }

    pub fn contains_named_graph(&self, graph_name: EncodedObjectId) -> bool {
        self.named_graphs.contains(&graph_name)
    }

    pub fn clear(&mut self) {
        for index in self.indexes.iter_mut() {
            index.clear();
        }
    }

    pub fn clear_graph(&mut self, graph_name: EncodedGraphObjectId) {
        for index in self.indexes.iter_mut() {
            index.clear_graph(graph_name);
        }
    }

    pub fn drop_named_graph(&mut self, graph_name: EncodedObjectId) -> bool {
        self.clear_graph(EncodedGraphObjectId(graph_name));
        self.named_graphs.remove(&graph_name)
    }

    fn any_index(&self) -> &MemQuadIndex {
        self.indexes.first().unwrap()
    }
}

/// A [MemQuadIndexScanIterator] that uses an [IndexSet] to choose the index for scanning.
pub struct MemQuadIndexSetScanIterator {
    /// The schema of the result.
    schema: SchemaRef,
    /// The inner iterator.
    inner: MemQuadIndexScanIterator<IndexRefInSet>,
}

impl MemQuadIndexSetScanIterator {
    /// Creates a new [MemQuadIndexSetScanIterator].
    pub fn new(
        schema: SchemaRef,
        index_set: Arc<OwnedRwLockReadGuard<IndexSet>>,
        index: IndexConfiguration,
        instructions: Box<IndexScanInstructions>,
    ) -> Self {
        let instructions = reorder_pattern(&instructions, &index.components);
        let iterator = MemQuadIndexScanIterator::new_from_index_set(
            index_set,
            index,
            instructions.clone(),
        );
        MemQuadIndexSetScanIterator {
            schema,
            inner: iterator,
        }
    }
}

impl Iterator for MemQuadIndexSetScanIterator {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next()?;
        let reordered = reorder_result(&self.schema, next.columns);
        Some(
            RecordBatch::try_new_with_options(
                Arc::clone(&self.schema),
                reordered,
                &RecordBatchOptions::new().with_row_count(Some(next.num_rows)),
            )
            .expect("Creates valid record batches"),
        )
    }
}

/// Re-orders the given `pattern` for the given `components`.
fn reorder_quad(pattern: &IndexedQuad, components: &IndexComponents) -> IndexedQuad {
    IndexedQuad([
        pattern.0[components.inner()[0].gspo_index()],
        pattern.0[components.inner()[1].gspo_index()],
        pattern.0[components.inner()[2].gspo_index()],
        pattern.0[components.inner()[3].gspo_index()],
    ])
}

/// Re-orders the given `pattern` for the given `components`.
fn reorder_pattern(
    pattern: &IndexScanInstructions,
    components: &IndexComponents,
) -> IndexScanInstructions {
    IndexScanInstructions([
        pattern.0[components.inner()[0].gspo_index()].clone(),
        pattern.0[components.inner()[1].gspo_index()].clone(),
        pattern.0[components.inner()[2].gspo_index()].clone(),
        pattern.0[components.inner()[3].gspo_index()].clone(),
    ])
}

/// Re-orders the given `pattern` for the given `components`.
fn reorder_result(
    schema: &Schema,
    columns: HashMap<String, Arc<dyn Array>>,
) -> Vec<Arc<dyn Array>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            Arc::clone(
                columns
                    .get(field.name())
                    .expect("Column must exist for scan"),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::storage::index::{
        IndexScanInstruction, IndexScanInstructions, IndexScanPredicate,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Fields};
    use insta::assert_debug_snapshot;
    use rdf_fusion_encoding::object_id::ObjectIdEncoding;
    use tokio::sync::RwLock;

    #[test]
    fn choose_index_all_bound() {
        let set = create_index_set();

        let pattern = IndexScanInstructions([
            traverse_and_filter(1),
            traverse_and_filter(2),
            traverse_and_filter(3),
            traverse_and_filter(4),
        ]);

        let result = set.choose_index(&pattern);

        assert_eq!(result.components, IndexComponents::GSPO);
    }

    #[test]
    fn choose_index_scan_predicate() {
        let set = create_index_set();

        let pattern = IndexScanInstructions([
            traverse_and_filter(0),
            traverse_and_filter(1),
            IndexScanInstruction::Scan(Arc::new("predicate".to_string()), None),
            traverse_and_filter(3),
        ]);

        let result = set.choose_index(&pattern);
        assert_eq!(result.components, IndexComponents::GOSP);
    }

    #[test]
    fn choose_index_scan_subject_and_object() {
        let set = create_index_set();

        let pattern = IndexScanInstructions([
            traverse_and_filter(0),
            IndexScanInstruction::Scan(Arc::new("subject".to_string()), None),
            traverse_and_filter(2),
            IndexScanInstruction::Scan(Arc::new("object".to_string()), None),
        ]);

        let result = set.choose_index(&pattern);

        assert_eq!(result.components, IndexComponents::GPOS);
    }

    #[test]
    fn test_reorder_quad_gspo() {
        let quad = dummy_quad();
        let reordered = reorder_quad(&quad, &IndexComponents::GSPO);
        assert_eq!(reordered.0, [oid(1), oid(2), oid(3), oid(4)]);
    }

    #[test]
    fn test_reorder_quad_gpos() {
        let quad = dummy_quad();
        let reordered = reorder_quad(&quad, &IndexComponents::GPOS);
        assert_eq!(reordered.0, [oid(1), oid(3), oid(4), oid(2)]);
    }

    #[test]
    fn test_reorder_pattern_gspo() {
        let pattern = IndexScanInstructions([
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Scan(Arc::new("object".to_string()), None),
        ]);
        let reordered = reorder_pattern(&pattern, &IndexComponents::GSPO);
        assert_eq!(reordered.0, pattern.0);
    }

    #[test]
    fn test_reorder_pattern_gpos() {
        let pattern = IndexScanInstructions([
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Traverse(None),
            IndexScanInstruction::Scan(Arc::new("O".to_string()), None),
        ]);

        let reordered = reorder_pattern(&pattern, &IndexComponents::GPOS);

        assert_eq!(
            reordered.0,
            [
                IndexScanInstruction::Traverse(None),
                IndexScanInstruction::Traverse(None),
                IndexScanInstruction::Scan(Arc::new("O".to_string()), None),
                IndexScanInstruction::Traverse(None),
            ]
        );
    }

    #[tokio::test]
    async fn scan_gpos_subject_and_object() {
        let set = RwLock::new(create_index_set());
        set.write()
            .await
            .insert(&[EncodedQuad {
                graph_name: EncodedGraphObjectId(EncodedObjectId::from(1)),
                subject: EncodedObjectId::from(2),
                predicate: EncodedObjectId::from(3),
                object: EncodedObjectId::from(4),
            }])
            .unwrap();

        let pattern = Box::new(IndexScanInstructions([
            traverse_and_filter(1),
            IndexScanInstruction::Scan(Arc::new("subject".to_string()), None),
            traverse_and_filter(3),
            IndexScanInstruction::Scan(Arc::new("object".to_string()), None),
        ]));

        let schema = Arc::new(Schema::new(Fields::from(vec![
            Field::new("subject", DataType::UInt32, false),
            Field::new("object", DataType::UInt32, false),
        ])));

        let set_lock = Arc::new(Arc::new(set).read_owned().await);
        let configuration = set_lock.choose_index(&pattern);

        let mut scan = MemQuadIndexSetScanIterator::new(
            schema,
            set_lock,
            configuration.clone(),
            pattern,
        );

        let batch = scan.next().unwrap();
        assert_eq!(configuration.components, IndexComponents::GPOS);
        assert_debug_snapshot!(batch, @r#"
        RecordBatch {
            schema: Schema {
                fields: [
                    Field {
                        name: "subject",
                        data_type: UInt32,
                        nullable: false,
                        dict_id: 0,
                        dict_is_ordered: false,
                        metadata: {},
                    },
                    Field {
                        name: "object",
                        data_type: UInt32,
                        nullable: false,
                        dict_id: 0,
                        dict_is_ordered: false,
                        metadata: {},
                    },
                ],
                metadata: {},
            },
            columns: [
                PrimitiveArray<UInt32>
                [
                  2,
                ],
                PrimitiveArray<UInt32>
                [
                  4,
                ],
            ],
            row_count: 1,
        }
        "#);
    }

    fn dummy_quad() -> IndexedQuad {
        IndexedQuad([oid(1), oid(2), oid(3), oid(4)])
    }

    fn create_index_set() -> IndexSet {
        IndexSet::new(
            ObjectIdEncoding::new(4),
            100,
            &[
                IndexComponents::GSPO,
                IndexComponents::GPOS,
                IndexComponents::GOSP,
            ],
        )
    }

    fn traverse_and_filter(id: u32) -> IndexScanInstruction {
        IndexScanInstruction::Traverse(Some(
            IndexScanPredicate::In([oid(id)].into()).into(),
        ))
    }

    fn oid(id: u32) -> EncodedObjectId {
        EncodedObjectId::from(id)
    }
}
