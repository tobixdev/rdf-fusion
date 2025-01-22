use crate::encoded::udfs::binary_dispatch::{dispatch_binary, EncScalarBinaryUdf};
use crate::encoded::{EncRdfTermBuilder, ENC_TYPE_TERM};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

pub const ENC_EQ: &str = "enc_same_term";
pub const ENC_GREATER_THAN: &str = "enc_greater_than";
pub const ENC_GREATER_OR_EQUAL: &str = "enc_greater_or_equal";
pub const ENC_LESS_THAN: &str = "enc_less_than";
pub const ENC_LESS_OR_EQUAL: &str = "enc_less_or_equal";

macro_rules! create_binary_cmp_udf {
    ($STRUCT: ident, $NAME: expr, $OP: tt) => {
        #[derive(Debug)]
        pub struct $STRUCT {
            signature: Signature,
        }

        impl $STRUCT {
            pub fn new() -> Self {
                Self {
                    signature: Signature::new(
                        TypeSignature::Exact(vec![ENC_TYPE_TERM.clone(), ENC_TYPE_TERM.clone()]),
                        Volatility::Immutable,
                    ),
                }
            }
        }

        impl EncScalarBinaryUdf for $STRUCT {
            type Collector = EncRdfTermBuilder;

            fn supports_named_node() -> bool {
                true
            }

            fn supports_blank_node() -> bool {
                true
            }

            fn supports_numeric() -> bool {
                true
            }

            fn supports_boolean() -> bool {
                true
            }

            fn supports_string() -> bool {
                true
            }

            fn supports_date_time() -> bool {
                true
            }

            fn supports_simple_literal() -> bool {
                true
            }

            fn supports_typed_literal() -> bool {
                true
            }

            fn eval_named_node(collector: &mut EncRdfTermBuilder, lhs: &str, rhs: &str) -> DFResult<()> {
                Ok(collector.append_boolean(lhs $OP rhs)?)
            }

            fn eval_blank_node(collector: &mut EncRdfTermBuilder, lhs: &str, rhs: &str) -> DFResult<()> {
                Ok(collector.append_boolean(lhs $OP rhs)?)
            }

            fn eval_numeric_i32(collector: &mut EncRdfTermBuilder, lhs: i32, rhs: i32) -> DFResult<()> {
                Ok(collector.append_boolean(lhs $OP rhs)?)
            }

            fn eval_numeric_i64(collector: &mut EncRdfTermBuilder, lhs: i64, rhs: i64) -> DFResult<()> {
                Ok(collector.append_boolean(lhs $OP rhs)?)
            }

            fn eval_numeric_f32(collector: &mut EncRdfTermBuilder, lhs: f32, rhs: f32) -> DFResult<()>  {
                Ok(collector.append_boolean(lhs $OP rhs)?)
            }

            fn eval_numeric_f64(collector: &mut EncRdfTermBuilder, lhs: f64, rhs: f64) -> DFResult<()> {
                Ok(collector.append_boolean(lhs $OP rhs)?)
            }

            fn eval_boolean(collector: &mut EncRdfTermBuilder, lhs: bool, rhs: bool) -> DFResult<()> {
                Ok(collector.append_boolean(lhs $OP rhs)?)
            }

            fn eval_string(collector: &mut EncRdfTermBuilder, lhs: &str, rhs: &str) -> DFResult<()> {
                Ok(collector.append_boolean(lhs $OP rhs)?)
            }

            fn eval_simple_literal(collector: &mut EncRdfTermBuilder, lhs: &str, rhs: &str) -> DFResult<()> {
                Ok(collector.append_boolean(lhs $OP rhs)?)
            }

            fn eval_typed_literal(
                collector: &mut EncRdfTermBuilder,
                lhs: &str,
                lhs_type: &str,
                rhs: &str,
                rhs_type: &str
            ) -> DFResult<()> {
                Ok(collector.append_boolean(lhs $OP rhs && lhs_type $OP rhs_type)?)
            }

            fn eval_incompatible(
                collector: &mut EncRdfTermBuilder
            ) -> DFResult<()> {
                Ok(collector.append_null()?)
            }
        }

        impl ScalarUDFImpl for $STRUCT {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn name(&self) -> &str {
                $NAME
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
                Ok(ENC_TYPE_TERM.clone())
            }

            fn invoke_batch(
                &self,
                args: &[ColumnarValue],
                number_rows: usize,
            ) -> datafusion::common::Result<ColumnarValue> {
                dispatch_binary::<Self>(args, number_rows)
            }
        }
    };
}

create_binary_cmp_udf!(EncEq, ENC_EQ, ==);
create_binary_cmp_udf!(EncGreaterThan, ENC_GREATER_THAN, >);
create_binary_cmp_udf!(EncGreaterOrEqual, ENC_GREATER_OR_EQUAL, >=);
create_binary_cmp_udf!(EncLessThan, ENC_LESS_THAN, <);
create_binary_cmp_udf!(EncLessOrEqual, ENC_LESS_OR_EQUAL, <=);

#[derive(Debug)]
pub struct EncSameTerm {
    signature: Signature,
}

impl EncSameTerm {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![ENC_TYPE_TERM.clone(), ENC_TYPE_TERM.clone()]),
                Volatility::Immutable,
            ),
        }
    }
}

impl EncScalarBinaryUdf for EncSameTerm {
    type Collector = EncRdfTermBuilder;

    fn supports_named_node() -> bool {
        true
    }

    fn supports_blank_node() -> bool {
        true
    }

    fn supports_numeric() -> bool {
        false
    }

    fn supports_boolean() -> bool {
        false
    }

    fn supports_string() -> bool {
        false
    }

    fn supports_date_time() -> bool {
        false
    }

    fn supports_simple_literal() -> bool {
        false
    }

    fn supports_typed_literal() -> bool {
        true
    }

    fn eval_named_node(collector: &mut EncRdfTermBuilder, lhs: &str, rhs: &str) -> DFResult<()> {
        Ok(collector.append_boolean(lhs == rhs)?)
    }

    fn eval_blank_node(collector: &mut EncRdfTermBuilder, lhs: &str, rhs: &str) -> DFResult<()> {
        Ok(collector.append_boolean(lhs == rhs)?)
    }

    fn eval_numeric_i32(_collector: &mut EncRdfTermBuilder, _lhs: i32, _rhs: i32) -> DFResult<()> {
        panic!("eval_numeric_i32 not supported!")
    }

    fn eval_numeric_i64(_collector: &mut EncRdfTermBuilder, _lhs: i64, _rhs: i64) -> DFResult<()> {
        panic!("eval_numeric_i64 not supported!")
    }

    fn eval_numeric_f32(_collector: &mut EncRdfTermBuilder, _lhs: f32, _rhs: f32) -> DFResult<()> {
        panic!("eval_numeric_f32 not supported!")
    }

    fn eval_numeric_f64(_collector: &mut EncRdfTermBuilder, _lhs: f64, _rhs: f64) -> DFResult<()> {
        panic!("eval_numeric_f64 not supported!")
    }

    fn eval_boolean(_collector: &mut EncRdfTermBuilder, _lhs: bool, _rhs: bool) -> DFResult<()> {
        panic!("eval_boolean not supported!")
    }

    fn eval_string(_collector: &mut EncRdfTermBuilder, _lhs: &str, _rhs: &str) -> DFResult<()> {
        panic!("eval_string not supported!")
    }

    fn eval_simple_literal(
        _collector: &mut EncRdfTermBuilder,
        _lhs: &str,
        _rhs: &str,
    ) -> DFResult<()> {
        panic!("eval_simple_literal not supported!")
    }

    fn eval_typed_literal(
        collector: &mut EncRdfTermBuilder,
        lhs: &str,
        lhs_type: &str,
        rhs: &str,
        rhs_type: &str,
    ) -> DFResult<()> {
        Ok(collector.append_boolean(lhs == rhs && lhs_type == rhs_type)?)
    }

    fn eval_incompatible(collector: &mut EncRdfTermBuilder) -> DFResult<()> {
        Ok(collector.append_null()?)
    }
}

impl ScalarUDFImpl for EncSameTerm {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "enc_same_term"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(ENC_TYPE_TERM.clone())
    }

    fn invoke_batch(
        &self,
        args: &[ColumnarValue],
        number_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        // Maybe we can improve this as we don't need the casting for same term (I think)
        dispatch_binary::<Self>(args, number_rows)
    }
}
