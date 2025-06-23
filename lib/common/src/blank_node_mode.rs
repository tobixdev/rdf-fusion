/// TODO
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum BlankNodeMatchingMode {
    /// A blank node is interpreted as a variable.
    #[default]
    Variable,
    /// A blank node is interpreted as a constant filter.
    Filter,
}
