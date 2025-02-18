use crate::result_collector::ResultCollector;
use crate::sorting::{RdfTermSortField, FIELDS_RDF_TERM_SORT};
use crate::DFResult;
use datafusion::arrow::array::{
    ArrayBuilder, Float64Builder, StringBuilder, StructBuilder, UInt8Builder,
};
use datafusion::logical_expr::ColumnarValue;
use std::sync::Arc;

enum RdfSortType {
    Null,
    BlankNodes,
    Iri,
    Numeric,
    String,
}

impl RdfSortType {
    pub fn as_u8(&self) -> u8 {
        match self {
            RdfSortType::Null => 0,
            RdfSortType::BlankNodes => 1,
            RdfSortType::Iri => 2,
            RdfSortType::Numeric => 3,
            RdfSortType::String => 4,
        }
    }
}

pub struct RdfTermSortBuilder {
    sort_builder: StructBuilder,
}

impl RdfTermSortBuilder {
    pub fn append_null(&mut self) {
        self.append(RdfSortType::Null, None, None)
    }

    pub fn append_numeric(&mut self, value: f64) {
        // TODO this will not work in some cases
        self.append(RdfSortType::Numeric, Some(value), None)
    }

    pub fn append_blank_node(&mut self, value: &str) {
        self.append(RdfSortType::BlankNodes, None, Some(value))
    }

    pub fn append_iri(&mut self, value: &str) {
        self.append(RdfSortType::Iri, None, Some(value))
    }

    pub fn append_string(&mut self, value: &str) {
        self.append(RdfSortType::String, None, Some(value))
    }

    fn append(&mut self, sort_type: RdfSortType, numeric: Option<f64>, string: Option<&str>) {
        self.sort_builder
            .field_builder::<UInt8Builder>(RdfTermSortField::Type.index())
            .unwrap()
            .append_value(sort_type.as_u8());

        let numeric_builder = self
            .sort_builder
            .field_builder::<Float64Builder>(RdfTermSortField::Numeric.index())
            .unwrap();
        match numeric {
            None => numeric_builder.append_null(),
            Some(numeric) => numeric_builder.append_value(numeric),
        }

        let string_builder = self
            .sort_builder
            .field_builder::<StringBuilder>(RdfTermSortField::String.index())
            .unwrap();
        match string {
            None => string_builder.append_null(),
            Some(string) => string_builder.append_value(string),
        }
    }
}

impl ResultCollector for RdfTermSortBuilder {
    fn new() -> Self {
        Self {
            sort_builder: StructBuilder::from_fields(FIELDS_RDF_TERM_SORT.clone(), 0),
        }
    }

    fn finish_columnar_value(mut self) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Array(Arc::new(self.sort_builder.finish())))
    }
}
