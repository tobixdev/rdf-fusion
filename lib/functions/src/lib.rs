#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/tobixdev/rdf-fusion/main/misc/logo/logo.png"
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/tobixdev/rdf-fusion/main/misc/logo/logo.png"
)]

//! This crate implements the SPARQL scalar and aggregate functions for [RDF Fusion](https://docs.rs/rdf-fusion/).
//!
//! While all SPARQL functions are implemented as DataFusion user-defined functions (UDFs),
//! we also provide additional support to simplify SPARQL function implementation.
//!
//! # Scalar Functions
//!
//! To implement a scalar function, implement the [`ScalarSparqlOp`](scalar::ScalarSparqlOp) trait.
//! Then, use the [`ScalarSparqlOpAdapter`](scalar::ScalarSparqlOpAdapter) to make the SPARQL operation
//! compatible with DataFusion’s UDF system.
//!
//! # Aggregate Functions
//!
//! Aggregate functions currently have limited support.
//! They only support typed value encoding and do not yet provide a SPARQL-specific trait to simplify development.
//! We plan to provide enhanced support for aggregate functions in the future.
//!
//! # Dispatch
//!
//! Dispatch functions are a toolkit designed to help implement "iterative" versions of SPARQL functions
//! that operate on standard Rust types.
//! However, this functionality may be removed in the future for the following reasons:
//! 1. It often reduces performance compared to directly working on the arrays.
//! 2. In its current form, it is incompatible with some planned future improvements.

pub mod aggregates;
pub mod builtin;
pub mod registry;
pub mod scalar;

#[cfg(test)]
mod test_utils {
    use crate::registry::DefaultRdfFusionFunctionRegistry;
    use datafusion::logical_expr::ScalarUDF;
    use rdf_fusion_encoding::RdfFusionEncodings;
    use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
    use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
    use rdf_fusion_encoding::typed_value::{
        TYPED_VALUE_ENCODING, TypedValueArray, TypedValueArrayElementBuilder,
    };
    use rdf_fusion_extensions::functions::{
        BuiltinName, FunctionName, RdfFusionFunctionRegistry,
    };
    use rdf_fusion_model::{Decimal, NamedNodeRef};
    use std::sync::Arc;

    /// Creates a test vector with mixed types.
    pub(crate) fn create_mixed_test_vector() -> TypedValueArray {
        let mut test_vector = TypedValueArrayElementBuilder::default();
        test_vector
            .append_named_node(NamedNodeRef::new_unchecked("http://example.com/test"))
            .unwrap();
        test_vector.append_decimal(Decimal::from(10)).unwrap();
        test_vector.finish()
    }

    /// Creates an instance of the given builtin UDF.
    pub(crate) fn create_default_builtin_udf(name: BuiltinName) -> Arc<ScalarUDF> {
        let encodings = RdfFusionEncodings::new(
            PLAIN_TERM_ENCODING,
            TYPED_VALUE_ENCODING,
            None,
            SORTABLE_TERM_ENCODING,
        );
        let function_registry = DefaultRdfFusionFunctionRegistry::new(encodings);
        function_registry.udf(&FunctionName::Builtin(name)).unwrap()
    }
}
