use crate::{RdfOpResult, ScalarBinaryRdfOp};
use datamodel::{Integer, StringLiteralRef};

#[derive(Debug)]
pub struct SubStrRdfOp {}

impl SubStrRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for SubStrRdfOp {
    type ArgLhs<'lhs> = StringLiteralRef<'lhs>;
    type ArgRhs<'lhs> = Integer;
    type Result<'data> = StringLiteralRef<'data>;

    fn evaluate<'data>(
        &self,
        arg_lhs: Self::ArgLhs<'data>,
        arg_rhs: Self::ArgRhs<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        let index = usize::try_from(arg_rhs.try_as_i64()?).map_err(|_| ())?;

        // We want to slice on char indices, not byte indices
        let mut start_iter = arg_lhs.0
            .char_indices()
            .skip(index.checked_sub(1).ok_or(())?)
            .peekable();
        let result = if let Some((start_position, _)) = start_iter.peek().copied() {
            &arg_lhs.0[start_position..]
        } else {
            ""
        };

        Ok(StringLiteralRef(result, arg_lhs.1))
    }
}
