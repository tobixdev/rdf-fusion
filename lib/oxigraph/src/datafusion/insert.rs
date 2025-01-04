use crate::datafusion::arrow::{single_quad_table_schema_encode_quads, SINGLE_QUAD_TABLE_SCHEMA};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::prelude::SessionContext;
use oxrdf::Quad;
use std::sync::Arc;

pub async fn load_from_reader(
    ctx: &SessionContext,
    quads: Vec<Quad>,
) -> Result<(), DataFusionError> {
    let batches = single_quad_table_schema_encode_quads(quads)?;
    let schema = SchemaRef::new(SINGLE_QUAD_TABLE_SCHEMA.clone());

    let state = ctx.state();
    let source = MemoryExec::try_new(&[batches], schema, None)?;
    let physical_plan = ctx
        .table_provider("quads")
        .await?
        .insert_into(&state, Arc::new(source), InsertOp::Append)
        .await?;

    collect(physical_plan, ctx.task_ctx()).await?;

    Ok(())
}
