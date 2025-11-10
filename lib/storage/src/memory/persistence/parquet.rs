use crate::memory::persistence::{
    MemQuadPersistenceOptions, MemQuadStoragePersistence, MemStoragePersistenceError,
};
use crate::memory::storage::MemQuadStorageSnapshot;
use async_trait::async_trait;
use datafusion::arrow::array::{RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::config::ConfigOptions;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::logical_expr::ReturnFieldArgs;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExprRef, ScalarFunctionExpr};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use futures::{Stream, StreamExt};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_extensions::functions::{BuiltinName, FunctionName};
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_model::DFResult;
use std::fs::File;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

/// An implementation of [MemQuadStoragePersistence] for the Parquet file format.
///
/// A single Parquet file is used to store the entire quad storage.
///
/// # Limitations
///
/// Currently, only the [PlainTermEncoding](rdf_fusion_encoding::plain_term::PlainTermEncoding) is
/// supported. A translation of an object id-based [MemQuadStorageSnapshot] is supported.
pub struct ParquetMemQuadStoragePersistence {
    /// The RDF Fusion configuration.
    context: RdfFusionContextView,
    /// DataFusion configuration options.
    config_options: Arc<ConfigOptions>,
    /// The path of the Parquet file.
    file_path: PathBuf,
}

#[async_trait]
impl MemQuadStoragePersistence for ParquetMemQuadStoragePersistence {
    async fn export(
        &self,
        storage: &MemQuadStorageSnapshot,
        options: &MemQuadPersistenceOptions,
    ) -> Result<(), MemStoragePersistenceError> {
        let encoding = options
            .encoding
            .clone()
            .unwrap_or(QuadStorageEncoding::PlainTerm);
        if encoding != QuadStorageEncoding::PlainTerm {
            return Err(MemStoragePersistenceError::UnsupportedEncoding(encoding));
        };

        let plain_term_stream = self.create_plain_term_stream(storage).await?;
        self.write_data_file(plain_term_stream).await?;

        Ok(())
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

                Ok(Box::pin(QuadEncodingChangeStream {
                    schema: quads.schema(),
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
            .context
            .functions()
            .udf(&FunctionName::Builtin(BuiltinName::WithPlainTermEncoding))?;
        let udf_name = udf.name().to_owned();

        let column = Arc::new(Column::new(column, column_index)) as PhysicalExprRef;
        let return_args = ReturnFieldArgs {
            arg_fields: schema.fields(),
            scalar_arguments: &[None, None, None, None],
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
    async fn write_data_file(
        &self,
        mut plain_term_stream: SendableRecordBatchStream,
    ) -> Result<(), MemStoragePersistenceError> {
        let file = File::create(self.file_path.clone())
            .map_err(|err| MemStoragePersistenceError::DataFileError(Box::new(err)))?;

        let mut writer = ArrowWriter::try_new(file, plain_term_stream.schema(), None)?;
        while let Some(batch) = plain_term_stream.next().await {
            writer.write(&batch?)?;
        }
        writer.finish()?;
        Ok(())
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

    #[test]
    fn test_quad_encoding_stream() {
        todo!()
    }
}
