use datafusion::arrow::datatypes::DataType;
use datafusion::error::DataFusionError;
use thiserror::Error;

#[derive(Debug, Clone, Error)]
#[error("Could not create result: {0}")]
pub enum TypedFamilyPureResultError {
    #[error(
        "The data type of the array does not match the expected type of the typed value family. Expected: {0}, Actual: {1}"
    )]
    DataTypeDoesNotMatch(DataType, DataType),
}

impl From<TypedFamilyPureResultError> for DataFusionError {
    fn from(value: TypedFamilyPureResultError) -> Self {
        DataFusionError::External(Box::new(value))
    }
}

#[derive(Debug, Error)]
#[error("Could not create extensible typed value operation: {0}")]
pub enum ExtensibleTypedValueSparqlOpCreationError {
    #[error("The type family '{0}' is not registered.")]
    UnregisteredFamily(String),
    #[error("Could not extract family mapping from the given UDF '{0}'.")]
    CannotExtractFamilyMappingFromUDF(String),
    #[error("The type family '{0}', which is part of the signature, is not part of the encoding.")]
    UnknownTypeFamily(String),
}
