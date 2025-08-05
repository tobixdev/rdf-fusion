/// An object id uniquely identifies an RDF Term. As a result, two object ids with a = b imply that
/// this is the same RDF term. No further checks are necessary.
///
/// # Future Plans
///
/// Currently, the object id machinery only supports `u32`. However, in the future we plan to
/// support arbitrary length ids to enable experimentation (e.g., inlining small strings).
#[derive(Eq, PartialEq, Debug, Clone, Copy, Hash)]
pub struct ObjectId(pub u32);

impl From<u32> for ObjectId {
    fn from(id: u32) -> Self {
        ObjectId(id)
    }
}
