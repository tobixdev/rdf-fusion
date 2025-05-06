#[macro_export]
macro_rules! impl_nullary_op {
    ($ENCODING: ty, $ENCODER: ty, $STRUCT_NAME:ident, $SPARQL_OP:ty) => {
        #[derive(Debug)]
        struct $STRUCT_NAME {
            signature: Signature,
            op: $SPARQL_OP,
        }

        impl SparqlOpDispatcher for $STRUCT_NAME {
            fn name(&self) -> &str {
                self.op.name()
            }

            fn signature(&self) -> &Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
                Ok(<$ENCODING>::data_type())
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs<'_>) -> DFResult<ColumnarValue> {
                let results = (0..args.number_rows)
                    .into_iter()
                    .map(|_| self.op.evaluate().into());
                let result = <$ENCODER>::encode_terms(results)?;
                Ok(ColumnarValue::Array(EncodingArray::into_array(result)))
            }
        }
    };
}
