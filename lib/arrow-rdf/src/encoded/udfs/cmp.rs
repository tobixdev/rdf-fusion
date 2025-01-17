use crate::encoded::udfs::binary_dispatch::{dispatch_binary, EncScalarBinaryUdf};
use crate::encoded::{EncRdfTermBuilder, ENC_TYPE_TERM};
use crate::DFResult;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use std::any::Any;

pub const ENC_EQ: &str = "enc_eq";

macro_rules! create_binary_cmp_udf {
    ($STRUCT: ident, $NAME: expr, $TYPE: expr, $OP: tt) => {
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
        }

        impl ScalarUDFImpl for EncEq {
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
                Ok($TYPE)
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

create_binary_cmp_udf!(EncEq, ENC_EQ, ENC_TYPE_TERM.clone(), ==);
