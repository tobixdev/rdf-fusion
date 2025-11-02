//! Contains general data structures on quad indexes that can be applied to in-memory and on-disk
//! indexes.
//!
//! A quad index represents a particular sorting of the quad components graph name, subject,
//! predicate, and object. For example, the [IndexComponents::GSPO] index regresents that exact
//! ordering while the [IndexComponents::GPOS] has the predicate as the second component. Different
//! types of graph patterns may be better suited for different indexes.
//!
//! The primary trait in this module is the [QuadIndex]. We recommend users unfamiliar with the
//! architecture to explore this module from there. In addition, [IndexPermutations] provides access
//! to multiple indexes while implementing the reordering logic.

mod components;
mod error;
mod permutations;

pub use components::*;
pub use error::*;
pub use permutations::*;
use std::fmt::Debug;
use std::hash::Hash;

/// Represents a single instance of a quad index with a given ordering.
pub trait QuadIndex {
    /// The data structure that is used to represent a single RDF term.
    type Term: EncodedTerm;

    /// Implements the separate storage for named graphs. As a named graph may exist without any
    /// associated quads, this is a separate element.
    type NamedGraphStorage: NamedGraphStorage<Term = Self::Term>;

    /// The [ScanInstructions] that are used to scan the index.
    type ScanInstructions: ScanInstructions;

    /// Returns the components of the index.
    fn components(&self) -> IndexComponents;

    /// Returns the total number of quads.
    fn len(&self) -> usize;

    /// Computes the "scan score" for the given `instructions`.
    ///
    /// The higher the scan score, the better is the index suited for scanning a particular pattern.
    /// The [IndexPermutations] will select the index with the highest scan score for implementing
    /// a scan.
    fn compute_scan_score(&self, instructions: &Self::ScanInstructions) -> usize;

    /// Inserts a list of quads.
    ///
    /// Quads that already exist in the index are ignored.
    fn insert(&mut self, quads: impl IntoIterator<Item = IndexQuad<Self::Term>>)
    -> usize;

    /// Removes a list of quads.
    ///
    /// Quads that do not exist in the index are ignored.
    fn remove(&mut self, quads: impl IntoIterator<Item = IndexQuad<Self::Term>>)
    -> usize;

    /// Clears the entire index
    fn clear(&mut self);

    /// Clears the given `graph_name`.
    fn clear_graph(&mut self, graph_name: Self::Term);
}

/// The data structure that is used to represent a single RDF term. This can be either an object id
/// or a plain term, depending on which types of elements the index holds.
///
/// The term encoding should have a special value for the default graph.
pub trait EncodedTerm:
    Debug + Clone + Copy + PartialEq + Eq + Hash + PartialOrd + Ord
{
    /// Returns true if the encoded term is the default graph.
    fn is_default_graph(&self) -> bool;
}

/// Implements a separate storage for named graphs. As a named graph may exist without any
/// associated quads, this must be a separate data structure.
pub trait NamedGraphStorage {
    /// This is the same term type as the quad index itself.
    type Term;

    /// Returns true if the `graph_name` is a named graph and part of this [NamedGraphStorage].
    fn contains(&self, graph_name: &Self::Term) -> bool;

    /// Returns an iterator over all named graphs in the [NamedGraphStorage].
    fn iter(&self) -> impl Iterator<Item = Self::Term>;

    /// Inserts the `graph_name` into the [NamedGraphStorage].
    fn insert(&mut self, graph_name: Self::Term) -> bool;

    /// Removes the `graph_name` from the [NamedGraphStorage].
    fn remove(&mut self, graph_name: &Self::Term) -> bool;
}

/// Scan instructions are used to implement a scan. The scan instructions should capture which parts
/// of the quad are bound to variables and which parts should be filtered.
pub trait ScanInstructions {
    /// Reorders the [ScanInstructions] to the given `components`.
    fn reorder(&self, components: IndexComponents) -> Self;
}
