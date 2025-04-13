#[macro_export]
macro_rules! make_nullary_rdf_udf {
    ($IMPL_TYPE: ty, $STRUCT_NAME: ident, $CONST_NAME: ident, $NAME: literal) => {
        #[derive(Debug)]
        pub struct $STRUCT_NAME {
            signature: datafusion::logical_expr::Signature,
            implementation: $IMPL_TYPE,
        }

        impl $STRUCT_NAME {
            pub fn new() -> Self {
                Self {
                    signature: datafusion::logical_expr::Signature::new(
                        datafusion::logical_expr::TypeSignature::Nullary,
                        datafusion::logical_expr::Volatility::Volatile,
                    ),
                    implementation: <$IMPL_TYPE>::new(),
                }
            }
        }

        impl datafusion::logical_expr::ScalarUDFImpl for $STRUCT_NAME {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn name(&self) -> &str {
                $NAME
            }

            fn signature(&self) -> &datafusion::logical_expr::Signature {
                &self.signature
            }

            fn return_type(
                &self,
                _arg_types: &[datafusion::arrow::datatypes::DataType],
            ) -> crate::DFResult<datafusion::arrow::datatypes::DataType> {
                Ok(crate::EncTerm::data_type())
            }

            fn invoke_batch(
                &self,
                _args: &[datafusion::physical_plan::ColumnarValue],
                number_rows: usize,
            ) -> datafusion::common::Result<datafusion::physical_plan::ColumnarValue> {
                let results = (0..number_rows)
                    .into_iter()
                    .map(|_| functions_scalar::ScalarNullaryRdfOp::evaluate(&self.implementation));

                let result =
                    <$IMPL_TYPE as functions_scalar::ScalarNullaryRdfOp>::Result::iter_into_array(
                        results,
                    )?;
                Ok(datafusion::physical_plan::ColumnarValue::Array(result))
            }
        }

        pub const $CONST_NAME: once_cell::sync::Lazy<datafusion::logical_expr::ScalarUDF> =
            once_cell::sync::Lazy::new(|| {
                datafusion::logical_expr::ScalarUDF::from($STRUCT_NAME::new())
            });
    };
}

#[macro_export]
macro_rules! make_unary_rdf_udf {
    ($IMPL_TYPE: ty, $STRUCT_NAME: ident, $CONST_NAME: ident, $NAME: literal) => {
        crate::make_rdf_udf!(
            $IMPL_TYPE,
            $STRUCT_NAME,
            $CONST_NAME,
            $NAME,
            crate::encoded::dispatch::dispatch_unary,
            datafusion::logical_expr::TypeSignature::Exact(vec![crate::EncTerm::data_type()])
        );
    };
}

#[macro_export]
macro_rules! make_binary_rdf_udf {
    ($IMPL_TYPE: ty, $STRUCT_NAME: ident, $CONST_NAME: ident, $NAME: literal) => {
        crate::make_rdf_udf!(
            $IMPL_TYPE,
            $STRUCT_NAME,
            $CONST_NAME,
            $NAME,
            crate::encoded::dispatch::dispatch_binary,
            datafusion::logical_expr::TypeSignature::Exact(vec![crate::EncTerm::data_type(); 2])
        );
    };
}

#[macro_export]
macro_rules! make_ternary_rdf_udf {
    ($IMPL_TYPE: ty, $STRUCT_NAME: ident, $CONST_NAME: ident, $NAME: literal) => {
        crate::make_rdf_udf!(
            $IMPL_TYPE,
            $STRUCT_NAME,
            $CONST_NAME,
            $NAME,
            crate::encoded::dispatch::dispatch_ternary,
            datafusion::logical_expr::TypeSignature::Exact(vec![crate::EncTerm::data_type(); 3])
        );
    };
}

#[macro_export]
macro_rules! make_quaternary_rdf_udf {
    ($IMPL_TYPE: ty, $STRUCT_NAME: ident, $CONST_NAME: ident, $NAME: literal) => {
        crate::make_rdf_udf!(
            $IMPL_TYPE,
            $STRUCT_NAME,
            $CONST_NAME,
            $NAME,
            crate::encoded::dispatch::dispatch_quaternary,
            datafusion::logical_expr::TypeSignature::Exact(vec![crate::EncTerm::data_type(); 4])
        );
    };
}

#[macro_export]
macro_rules! make_n_ary_rdf_udf {
    ($IMPL_TYPE: ty, $STRUCT_NAME: ident, $CONST_NAME: ident, $NAME: literal) => {
        crate::make_rdf_udf!(
            $IMPL_TYPE,
            $STRUCT_NAME,
            $CONST_NAME,
            $NAME,
            crate::encoded::dispatch::dispatch_n_ary,
            datafusion::logical_expr::TypeSignature::OneOf(vec![
                datafusion::logical_expr::TypeSignature::Nullary,
                datafusion::logical_expr::TypeSignature::Variadic(
                    vec![crate::EncTerm::data_type()]
                )
            ])
        );
    };
}

#[macro_export]
macro_rules! make_rdf_udf {
    ($IMPL_TYPE: ty, $STRUCT_NAME: ident, $CONST_NAME: ident, $NAME: literal, $DISPATCH: expr, $SIGNATURE: expr) => {
        #[derive(Debug)]
        pub struct $STRUCT_NAME {
            signature: datafusion::logical_expr::Signature,
            implementation: $IMPL_TYPE,
        }

        impl $STRUCT_NAME {
            pub fn new() -> Self {
                Self {
                    signature: datafusion::logical_expr::Signature::new(
                        $SIGNATURE,
                        datafusion::logical_expr::Volatility::Immutable,
                    ),
                    implementation: <$IMPL_TYPE>::new(),
                }
            }
        }

        impl datafusion::logical_expr::ScalarUDFImpl for $STRUCT_NAME {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn name(&self) -> &str {
                $NAME
            }

            fn signature(&self) -> &datafusion::logical_expr::Signature {
                &self.signature
            }

            fn return_type(
                &self,
                _arg_types: &[datafusion::arrow::datatypes::DataType],
            ) -> crate::DFResult<datafusion::arrow::datatypes::DataType> {
                Ok(crate::EncTerm::data_type())
            }

            fn invoke_batch(
                &self,
                args: &[datafusion::physical_plan::ColumnarValue],
                number_rows: usize,
            ) -> datafusion::common::Result<datafusion::physical_plan::ColumnarValue> {
                $DISPATCH(&self.implementation, args, number_rows)
            }
        }

        pub const $CONST_NAME: once_cell::sync::Lazy<datafusion::logical_expr::ScalarUDF> =
            once_cell::sync::Lazy::new(|| {
                datafusion::logical_expr::ScalarUDF::from($STRUCT_NAME::new())
            });
    };
}
