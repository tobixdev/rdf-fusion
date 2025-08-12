use crate::memory::encoding::{EncodedQuad, EncodedTriplePattern};
use crate::memory::storage::stream::extract_columns;
use datafusion::common::Column;
use rdf_fusion_common::BlankNodeMatchingMode;
use rdf_fusion_model::Variable;
use std::collections::HashMap;

pub struct QuadEqualities(Vec<[u8; 4]>);

impl QuadEqualities {
    /// Creates a new [QuadEqualities] fom the variables of the quads.
    pub fn try_new(
        graph_variable: Option<&Variable>,
        pattern: &EncodedTriplePattern,
        blank_node_matching_mode: BlankNodeMatchingMode,
    ) -> Option<Self> {
        let vars = extract_columns(graph_variable, pattern, blank_node_matching_mode);

        let mut mapping: HashMap<&Column, [u8; 4]> = HashMap::new();

        for i in 0..4 {
            if let Some(var) = &vars[i] {
                let value = mapping.entry(var).or_insert([0; 4]);
                value[i] = 1;
            }
        }

        let equalities = mapping
            .into_iter()
            .filter_map(|(_, vars)| {
                let has_equality = vars.into_iter().filter(|v| *v == 1).count() > 1;
                has_equality.then_some(vars)
            })
            .collect::<Vec<_>>();

        if equalities.is_empty() {
            None
        } else {
            Some(Self(equalities))
        }
    }

    /// Filters the buffer in-place and fills up holes by moving all non-None entries to the front.
    /// After this, all `None` slots (holes) will be at the end of the buffer.
    pub fn remove_non_matching_quads(&self, quads: &mut [Option<EncodedQuad>; 32]) {
        let mut write_idx = 0;

        // Iterate over the buffer and write any matching quad to the write position.
        for read_idx in 0..quads.len() {
            if let Some(quad) = &mut quads[read_idx] {
                if self.evaluate(quad) {
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

    /// Evaluates whether the equalities hold for `quad`.
    fn evaluate(&self, quad: &EncodedQuad) -> bool {
        for equality in &self.0 {
            for i in 0..4 {
                for j in (i + 1)..4 {
                    if equality[i] == 1 && equality[j] == 1 {
                        let quad = [
                            quad.graph_name.0,
                            quad.subject,
                            quad.predicate,
                            quad.object,
                        ];

                        if quad[i] != quad[j] {
                            return false;
                        }
                    }
                }
            }
        }

        true
    }
}
