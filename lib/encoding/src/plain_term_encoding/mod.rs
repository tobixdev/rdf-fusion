mod array;
pub mod decoders;
pub mod encoders;
mod encoding;
mod scalar;

pub use array::PlainTermArray;
pub use encoding::PlainTermEncoding;
pub use encoding::TermType;
pub use scalar::PlainTermScalar;
