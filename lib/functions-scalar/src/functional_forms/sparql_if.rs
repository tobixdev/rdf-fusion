use crate::{RdfOpResult, ScalarTernaryRdfOp};
use datamodel::{Boolean, TermRef};

#[derive(Debug)]
pub struct IfRdfOp {}

impl IfRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarTernaryRdfOp for IfRdfOp {
    type Arg0<'data> = Boolean;
    type Arg1<'data> = TermRef<'data>;
    type Arg2<'data> = TermRef<'data>;
    type Result<'data> = TermRef<'data>;

    fn evaluate<'data>(
        &self,
        test: Self::Arg0<'data>,
        if_true: Self::Arg1<'data>,
        if_false: Self::Arg2<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        Ok(match test.as_bool() {
            true => if_true,
            false => if_false,
        })
    }

    fn evaluate_error<'data>(
        &self,
        arg0: RdfOpResult<Self::Arg0<'data>>,
        arg1: RdfOpResult<Self::Arg1<'data>>,
        arg2: RdfOpResult<Self::Arg2<'data>>,
    ) -> RdfOpResult<Self::Result<'data>> {
        if arg0.is_err() {
            return Err(());
        }
        match arg0.unwrap().as_bool() {
            true => arg1,
            false => arg2,
        }
    }
}
