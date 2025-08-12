use crate::memory::encoding::{EncodedActiveGraph, EncodedQuad, EncodedTriplePattern};

/// Allows iterating over a buffer of quads and only yielding those that match the given pattern.
pub struct QuadFilter(Box<dyn Fn(&EncodedQuad) -> bool + Send + Sync>);

impl QuadFilter {
    /// Creates a new [QuadFilter].
    pub fn new(
        active_graph: &EncodedActiveGraph,
        pattern: &EncodedTriplePattern,
    ) -> Self {
        let predicate = Box::new(move |quad: &EncodedQuad| {
            let graph_ok = match &active_graph {
                EncodedActiveGraph::DefaultGraph => quad.graph_name.is_default_graph(),
                EncodedActiveGraph::AllGraphs => true,
                EncodedActiveGraph::Union(allowed_graphs) => {
                    allowed_graphs.contains(&quad.graph_name)
                }
                EncodedActiveGraph::AnyNamedGraph => quad.graph_name.is_named_graph(),
            };

            graph_ok
                && (pattern.subject.try_as_object_id() == Some(quad.subject))
                && (pattern.predicate.try_as_object_id() == Some(quad.predicate))
                && (pattern.object.try_as_object_id() == Some(quad.object))
        });
        QuadFilter(predicate)
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
