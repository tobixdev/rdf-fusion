use crate::{BinarySparqlOp, SparqlOp, TernarySparqlOp, ThinResult};
use graphfusion_model::{Integer, StringLiteralRef, ThinError};

#[derive(Debug)]
pub struct SubStrSparqlOp;

impl Default for SubStrSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl SubStrSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for SubStrSparqlOp {
}

impl BinarySparqlOp for SubStrSparqlOp {
    type ArgLhs<'lhs> = StringLiteralRef<'lhs>;
    type ArgRhs<'lhs> = Integer;
    type Result<'data> = StringLiteralRef<'data>;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        evaluate_substr(lhs, rhs, None)
    }
}

impl TernarySparqlOp for SubStrSparqlOp {
    type Arg0<'data> = StringLiteralRef<'data>;
    type Arg1<'data> = Integer;
    type Arg2<'data> = Integer;
    type Result<'data> = StringLiteralRef<'data>;

    fn evaluate<'data>(
        &self,
        arg0: Self::Arg0<'data>,
        arg1: Self::Arg1<'data>,
        arg2: Self::Arg2<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        evaluate_substr(arg0, arg1, Some(arg2))
    }
}

fn evaluate_substr(
    source: StringLiteralRef<'_>,
    starting_loc: Integer,
    length: Option<Integer>,
) -> ThinResult<StringLiteralRef<'_>> {
    let index = usize::try_from(starting_loc.as_i64())?;
    let length = length.map(|l| usize::try_from(l.as_i64())).transpose()?;

    // We want to slice on char indices, not byte indices
    let mut start_iter = source
        .0
        .char_indices()
        .skip(index.checked_sub(1).ok_or(ThinError::Expected)?)
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
