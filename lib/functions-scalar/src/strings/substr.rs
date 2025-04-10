use crate::{RdfOpResult, ScalarBinaryRdfOp, ScalarTernaryRdfOp};
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
        source: Self::ArgLhs<'data>,
        starting_loc: Self::ArgRhs<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        evaluate_substr(source, starting_loc, None)
    }
}

impl ScalarTernaryRdfOp for SubStrRdfOp {
    type Arg0<'data> = StringLiteralRef<'data>;
    type Arg1<'data> = Integer;
    type Arg2<'data> = Integer;
    type Result<'data> = StringLiteralRef<'data>;

    fn evaluate<'data>(
        &self,
        source: Self::Arg0<'data>,
        starting_loc: Self::Arg1<'data>,
        length: Self::Arg2<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        evaluate_substr(source, starting_loc, Some(length))
    }
}

fn evaluate_substr(
    source: StringLiteralRef<'_>,
    starting_loc: Integer,
    length: Option<Integer>,
) -> RdfOpResult<StringLiteralRef<'_>> {
    let index = usize::try_from(starting_loc.try_as_i64()?).map_err(|_| ())?;
    let length = length
        .map(|l| usize::try_from(l.try_as_i64()?).map_err(|_| ()))
        .transpose()
        .map_err(|_| ())?;

    // We want to slice on char indices, not byte indices
    let mut start_iter = source
        .0
        .char_indices()
        .skip(index.checked_sub(1).ok_or(())?)
        .peekable();
    let result = if let Some((start_position, _)) = start_iter.peek().copied() {
        if let Some(length) = length {
            let mut end_iter = start_iter.skip(length).peekable();
            if let Some((end_position, _)) = end_iter.peek() {
                &source.0[start_position..*end_position]
            } else {
                &source.0[start_position..]
            }
        } else {
            &source.0[start_position..]
        }
    } else {
        ""
    };

    Ok(StringLiteralRef(result, source.1))
}
