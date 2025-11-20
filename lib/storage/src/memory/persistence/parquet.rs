use crate::memory::persistence::{
    MemQuadPersistenceOptions, MemQuadStoragePersistence, MemStoragePersistenceError,
};
use crate::memory::storage::MemQuadStorageSnapshot;
use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::config::ConfigOptions;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::logical_expr::ReturnFieldArgs;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExprRef, ScalarFunctionExpr};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use futures::{Stream, StreamExt};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_extensions::functions::{
    BuiltinName, FunctionName, RdfFusionFunctionRegistryRef,
};
use rdf_fusion_model::DFResult;
use rdf_fusion_model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

/// An implementation of [MemQuadStoragePersistence] for the Parquet file format.
///
/// A single Parquet file is used to store the entire quad storage.
///
/// # Limitations
///
/// Currently, only the [PlainTermEncoding](rdf_fusion_encoding::plain_term::PlainTermEncoding) is
/// supported. A translation between encodings is supported.
pub struct ParquetMemQuadStoragePersistence {
    /// The function registry used to look-up conversion functions.
    registry: RdfFusionFunctionRegistryRef,
    /// DataFusion configuration options.
    config_options: Arc<ConfigOptions>,
}

impl ParquetMemQuadStoragePersistence {
    /// Creates a new [`ParquetMemQuadStoragePersistence`].
    pub fn new(
        registry: RdfFusionFunctionRegistryRef,
        config_options: Arc<ConfigOptions>,
    ) -> Self {
        Self {
            registry,
            config_options,
        }
    }
}

#[async_trait]
impl MemQuadStoragePersistence for ParquetMemQuadStoragePersistence {
    type Metadata = ParquetMetaData;

    async fn export<TWriter: Write + Send>(
        &self,
        writer: TWriter,
        storage: &MemQuadStorageSnapshot,
        options: &MemQuadPersistenceOptions,
    ) -> Result<Self::Metadata, MemStoragePersistenceError> {
        let encoding = options
            .encoding
            .clone()
            .unwrap_or(QuadStorageEncoding::PlainTerm);
        if encoding != QuadStorageEncoding::PlainTerm {
            return Err(MemStoragePersistenceError::UnsupportedEncoding(encoding));
        };

        let plain_term_stream = self.create_plain_term_stream(storage).await?;
        let metadata = self.write_data_file(writer, plain_term_stream).await?;

        Ok(metadata)
    }
}

impl ParquetMemQuadStoragePersistence {
    /// Creates a new [SendableRecordBatchStream] in the Plain Term Encoding. If necessary, a
    /// mapping function will be applied.
    async fn create_plain_term_stream(
        &self,
        storage: &MemQuadStorageSnapshot,
    ) -> DFResult<SendableRecordBatchStream> {
        let execution_plan_metrics = ExecutionPlanMetricsSet::new();
        let baseline_metrics = BaselineMetrics::new(&execution_plan_metrics, 0);

        let quads = storage.stream_quads().await?;
        let quads = quads.create_stream(baseline_metrics);

        match storage.storage_encoding() {
            QuadStorageEncoding::PlainTerm => Ok(quads),
            QuadStorageEncoding::ObjectId(_) => {
                let inner_schema = quads.schema();
                let exprs = vec![
                    self.create_mapping_function(&inner_schema, COL_GRAPH, 0)?,
                    self.create_mapping_function(&inner_schema, COL_SUBJECT, 1)?,
                    self.create_mapping_function(&inner_schema, COL_PREDICATE, 2)?,
                    self.create_mapping_function(&inner_schema, COL_OBJECT, 3)?,
                ];

                let fields: Vec<Field> = exprs
                    .iter()
                    .zip(inner_schema.fields())
                    .map(|(expr, field)| {
                        let result_field = expr
                            .return_field(&quads.schema())
                            .expect("Only mapping. Field should always be available.");
                        Field::new(
                            field.name(),
                            result_field.data_type().clone(),
                            result_field.is_nullable(),
                        )
                        .with_metadata(result_field.metadata().clone())
                    })
                    .collect();
                let schema = Arc::new(Schema::new(fields));

                Ok(Box::pin(QuadEncodingChangeStream {
                    schema,
                    inner: quads,
                    exprs,
                }))
            }
        }
    }

    /// Creates a [PhysicalExprRef] that returns the mapped version of the given `column`.
    fn create_mapping_function(
        &self,
        schema: &Schema,
        column: &str,
        column_index: usize,
    ) -> DFResult<PhysicalExprRef> {
        let udf = self
            .registry
            .udf(&FunctionName::Builtin(BuiltinName::WithPlainTermEncoding))?;
        let udf_name = udf.name().to_owned();

        let column = Arc::new(Column::new(column, column_index)) as PhysicalExprRef;
        let return_args = ReturnFieldArgs {
            arg_fields: &schema.fields()[column_index..column_index + 1],
            scalar_arguments: &[None],
        };
        let return_field = udf.return_field_from_args(return_args)?;

        Ok(Arc::new(ScalarFunctionExpr::new(
            &udf_name,
            udf,
            vec![column],
            return_field,
            Arc::clone(&self.config_options),
        )) as PhysicalExprRef)
    }

    /// Writes the Parquet data file for the given `plain_term_stream`.
    async fn write_data_file<TWriter: Write + Send>(
        &self,
        writer: TWriter,
        mut plain_term_stream: SendableRecordBatchStream,
    ) -> Result<ParquetMetaData, MemStoragePersistenceError> {
        let mut writer = ArrowWriter::try_new(writer, plain_term_stream.schema(), None)?;
        while let Some(batch) = plain_term_stream.next().await {
            writer.write(&batch?)?;
        }
        let metadata = writer.finish()?;

        Ok(metadata)
    }
}

/// A stream that applies an encoding change function to the input stream.
///
/// The function will be applied to all fields in the stream.
struct QuadEncodingChangeStream {
    /// The schema of the output stream.
    schema: SchemaRef,
    /// The input stream.
    inner: SendableRecordBatchStream,
    /// The functions that implement the mapping.
    exprs: Vec<PhysicalExprRef>,
}

impl QuadEncodingChangeStream {
    /// Transforms the `inner_batch` using the registered function.
    fn transform(&self, inner_batch: &RecordBatch) -> DFResult<RecordBatch> {
        let arrays = self
            .exprs
            .iter()
            .map(|expr| {
                expr.evaluate(inner_batch)
                    .and_then(|v| v.into_array(inner_batch.num_rows()))
            })
            .collect::<DFResult<Vec<_>>>()?;

        if arrays.is_empty() {
            let options =
                RecordBatchOptions::new().with_row_count(Some(inner_batch.num_rows()));
            RecordBatch::try_new_with_options(Arc::clone(&self.schema), arrays, &options)
                .map_err(Into::into)
        } else {
            RecordBatch::try_new(Arc::clone(&self.schema), arrays).map_err(Into::into)
        }
    }
}

impl Stream for QuadEncodingChangeStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let inner = ready!(self.inner.poll_next_unpin(cx));

        match inner {
            None => Poll::Ready(None),
            Some(Ok(batch)) => Poll::Ready(Some(Ok(self.transform(&batch)?))),
            Some(Err(error)) => Poll::Ready(Some(Err(error))),
        }
    }
}

impl RecordBatchStream for QuadEncodingChangeStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    // Tests for writing Parquet files can be found in the tests directory.

    use super::*;
    use datafusion::arrow::util::display::FormatOptions;
    use datafusion::arrow::util::pretty::pretty_format_batches_with_options;
    use datafusion::arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::physical_expr::expressions::{CastExpr, Column};
    use futures::StreamExt;
    use insta::assert_snapshot;

    /// Uses `cast` as a surrogate for the encoding change function.
    #[tokio::test]
    async fn test_quad_encoding_stream() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));

        let array = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
        let input_stream: SendableRecordBatchStream = Box::pin(MyOnceStream::new(batch));

        let col_expr: PhysicalExprRef = Arc::new(Column::new("col1", 0));
        let cast_expr: PhysicalExprRef =
            Arc::new(CastExpr::new(col_expr, DataType::Utf8, None));

        let new_schema =
            Arc::new(Schema::new(vec![Field::new("col1", DataType::Utf8, false)]));
        let mut stream = QuadEncodingChangeStream {
            schema: new_schema.clone(),
            inner: input_stream,
            exprs: vec![cast_expr],
        };

        let result_batch = stream.next().await.unwrap().unwrap();
        let options = FormatOptions::default().with_types_info(true);
        assert_snapshot!(
            pretty_format_batches_with_options(&[result_batch], &options).unwrap(),
            @r"
        +------+
        | col1 |
        | Utf8 |
        +------+
        | 1    |
        | 2    |
        | 3    |
        +------+
        "
        );
    }

    /// Sends a single record batch to the stream.
    struct MyOnceStream {
        schema: SchemaRef,
        inner: Option<RecordBatch>,
    }

    impl MyOnceStream {
        /// Creates a new [MyOnceStream] that will emit the `record_batch`.
        fn new(record_batch: RecordBatch) -> Self {
            Self {
                schema: Arc::clone(&record_batch.schema()),
                inner: Some(record_batch),
            }
        }
    }

    impl Stream for MyOnceStream {
        type Item = DFResult<RecordBatch>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.inner.take().map(Ok))
        }
    }

    impl RecordBatchStream for MyOnceStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }
}
