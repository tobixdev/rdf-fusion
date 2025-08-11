use crate::memory::encoding::EncodedQuad;
use crate::memory::storage::stream::extract_term;
use crate::memory::MemObjectIdMapping;
use rdf_fusion_common::BlankNodeMatchingMode;
use rdf_fusion_encoding::object_id::UnknownObjectIdError;
use rdf_fusion_logical::ActiveGraph;
use rdf_fusion_model::TriplePattern;
use std::collections::HashSet;

/// Allows iterating over a buffer of quads and only yielding those that match the given pattern.
pub struct QuadFilter(Box<dyn Fn(&EncodedQuad) -> bool + Send + Sync>);

impl QuadFilter {
    /// Creates a new [QuadFilter].
    pub fn try_new(
        object_id_mapping: &MemObjectIdMapping,
        active_graph: ActiveGraph,
        pattern: &TriplePattern,
        blank_node_matching_mode: BlankNodeMatchingMode,
    ) -> Result<Self, UnknownObjectIdError> {
        let subject = extract_term(
            object_id_mapping,
            &pattern.subject,
            blank_node_matching_mode,
        )?;
        let predicate = extract_term(
            object_id_mapping,
            &pattern.predicate.clone().into(),
            blank_node_matching_mode,
        )?;
        let object =
            extract_term(object_id_mapping, &pattern.object, blank_node_matching_mode)?;

        let allowed_graphs = match &active_graph {
            ActiveGraph::Union(graphs) => {
                let mut result = HashSet::new();
                for graph in graphs {
                    let object_id = object_id_mapping
                        .try_get_encoded_object_id_from_graph_name(graph.as_ref())
                        .ok_or(UnknownObjectIdError)?;
                    result.insert(object_id);
                }
                result
            }
            _ => HashSet::new(),
        };

        let predicate = Box::new(move |quad: &EncodedQuad| {
            let graph_ok = match &active_graph {
                ActiveGraph::DefaultGraph => quad.graph_name.is_default_graph(),
                ActiveGraph::AllGraphs => true,
                ActiveGraph::Union(_) => allowed_graphs.contains(&quad.graph_name),
                ActiveGraph::AnyNamedGraph => quad.graph_name.is_named_graph(),
            };

            graph_ok
                && (subject.is_none() || subject == Some(quad.subject))
                && (predicate.is_none() || predicate == Some(quad.predicate))
                && (object.is_none() || object == Some(quad.object))
        });
        Ok(QuadFilter(predicate))
    }

    /// Filters the buffer in-place and fills up holes by moving all non-None entries to the front.
    /// After this, all `None` slots (holes) will be at the end of the buffer.
    pub fn remove_non_matching_quads(&self, quads: &mut [Option<EncodedQuad>; 32]) {
        let mut write_idx = 0;

        // Iterate over the buffer and write any matching quad to the write position.
        for read_idx in 0..quads.len() {
            if let Some(quad) = &mut quads[read_idx] {
                if self.0(quad) {
                    if write_idx != read_idx {
                        quads[write_idx] = quads[read_idx].take();
                    }
                    write_idx += 1;
                }
            }
        }

        // Fill the rest with None
        for quad in quads.iter_mut().skip(write_idx) {
            *quad = None;
        }
    }
}
