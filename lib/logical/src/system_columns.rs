use datafusion::common::{Column, plan_err};
use rdf_fusion_api::functions::BuiltinName;
use rdf_fusion_common::DFResult;

/// System columns refer to columns that are known to the engine. It can use these columns to
/// enable or improve the query execution.
pub struct SystemColumns;

impl SystemColumns {
    /// When encoding changes are part of filter or extend nodes, the transformation is pushed down
    /// into a projection node that allows query nodes "higher up" in the query plan to re-use these
    /// results. The goal is to minimize expensive encoding changes.
    pub fn encoding_push_down(
        column: &Column,
        encoding_function: BuiltinName,
    ) -> DFResult<Column> {
        let encoding_nick_name = match encoding_function {
            BuiltinName::WithPlainTermEncoding => "pt",
            BuiltinName::WithTypedValueEncoding => "tv",
            BuiltinName::WithSortableEncoding => "sort",
            _ => {
                return plan_err!(
                    "Unsupported encoding function: {:?}",
                    encoding_function
                );
            }
        };

        Ok(Column::new_unqualified(format!(
            "_{column}_{encoding_nick_name}"
        )))
    }
}
