use crate::typed_value::TypedValueArrayBuilder;

use crate::error::LiteralEncodingError;
use crate::DFResult;
use crate::TermEncoder;
use crate::TermEncoding;
use crate::TypedValueEncoding;
use datafusion::common::exec_err;
use rdf_fusion_model::{BlankNode, Double, NamedNode};
use rdf_fusion_model::{
    BlankNodeRef, LiteralRef, NamedNodeRef, Numeric, SimpleLiteralRef, StringLiteralRef, ThinResult,
};
use rdf_fusion_model::{
    Boolean, DateTime, DayTimeDuration, Decimal, Float, Int, Integer, OwnedStringLiteral, ThinError,
};

#[macro_export]
macro_rules! make_simple_term_value_encoder {
    ($STRUCT_NAME: ident, $VALUE_TYPE: ty, $BUILDER_INVOCATION: expr) => {
        #[derive(Debug)]
        pub struct $STRUCT_NAME {}

        impl TermEncoder<TypedValueEncoding> for $STRUCT_NAME {
            type Term<'data> = $VALUE_TYPE;

            fn encode_terms<'data>(
                terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
            ) -> DFResult<<TypedValueEncoding as TermEncoding>::Array> {
                let mut builder = TypedValueArrayBuilder::default();
                for term_result in terms {
                    match term_result {
                        Ok(value) => $BUILDER_INVOCATION(&mut builder, value)?,
                        Err(ThinError::Expected) => builder.append_null()?,
                        Err(ThinError::InternalError(cause)) => {
                            return exec_err!("Internal error during RDF operation: {cause}")
                        }
                    }
                }
                TypedValueEncoding::try_new_array(builder.finish())
            }
        }
    };
}

make_simple_term_value_encoder!(
    NamedNodeTermValueEncoder,
    NamedNode,
    |builder: &mut TypedValueArrayBuilder, value: NamedNode| {
        builder.append_named_node(value.as_ref())
    }
);
make_simple_term_value_encoder!(
    NamedNodeRefTermValueEncoder,
    NamedNodeRef<'data>,
    |builder: &mut TypedValueArrayBuilder, value: NamedNodeRef<'data>| {
        builder.append_named_node(value)
    }
);
make_simple_term_value_encoder!(
    BlankNodeTermValueEncoder,
    BlankNode,
    |builder: &mut TypedValueArrayBuilder, value: BlankNode| {
        builder.append_blank_node(value.as_ref())
    }
);
make_simple_term_value_encoder!(
    BlankNodeRefTermValueEncoder,
    BlankNodeRef<'data>,
    |builder: &mut TypedValueArrayBuilder, value: BlankNodeRef<'data>| {
        builder.append_blank_node(value)
    }
);
make_simple_term_value_encoder!(
    BooleanTermValueEncoder,
    Boolean,
    |builder: &mut TypedValueArrayBuilder, value: Boolean| { builder.append_boolean(value) }
);
make_simple_term_value_encoder!(
    SimpleLiteralRefTermValueEncoder,
    SimpleLiteralRef<'data>,
    |builder: &mut TypedValueArrayBuilder, value: SimpleLiteralRef<'data>| {
        builder.append_string(value.value, None)
    }
);
make_simple_term_value_encoder!(
    StringLiteralRefTermValueEncoder,
    StringLiteralRef<'data>,
    |builder: &mut TypedValueArrayBuilder, value: StringLiteralRef<'data>| {
        builder.append_string(value.0, value.1)
    }
);
make_simple_term_value_encoder!(
    OwnedStringLiteralTermValueEncoder,
    OwnedStringLiteral,
    |builder: &mut TypedValueArrayBuilder, value: OwnedStringLiteral| {
        builder.append_string(value.0.as_str(), value.1.as_deref())
    }
);
make_simple_term_value_encoder!(
    IntTermValueEncoder,
    Int,
    |builder: &mut TypedValueArrayBuilder, value: Int| { builder.append_int(value) }
);
make_simple_term_value_encoder!(
    IntegerTermValueEncoder,
    Integer,
    |builder: &mut TypedValueArrayBuilder, value: Integer| { builder.append_integer(value) }
);
make_simple_term_value_encoder!(
    FloatTermValueEncoder,
    Float,
    |builder: &mut TypedValueArrayBuilder, value: Float| { builder.append_float(value) }
);
make_simple_term_value_encoder!(
    DoubleTermValueEncoder,
    Double,
    |builder: &mut TypedValueArrayBuilder, value: Double| { builder.append_double(value) }
);
make_simple_term_value_encoder!(
    DecimalTermValueEncoder,
    Decimal,
    |builder: &mut TypedValueArrayBuilder, value: Decimal| { builder.append_decimal(value) }
);
make_simple_term_value_encoder!(
    NumericTypedValueEncoder,
    Numeric,
    |builder: &mut TypedValueArrayBuilder, value: Numeric| { builder.append_numeric(value) }
);
make_simple_term_value_encoder!(
    DateTimeTermValueEncoder,
    DateTime,
    |builder: &mut TypedValueArrayBuilder, value: DateTime| { builder.append_date_time(value) }
);
make_simple_term_value_encoder!(
    DayTimeDurationTermValueEncoder,
    DayTimeDuration,
    |builder: &mut TypedValueArrayBuilder, value: DayTimeDuration| {
        builder.append_duration(None, Some(value))
    }
);
make_simple_term_value_encoder!(
    LiteralRefTermValueEncoder,
    LiteralRef<'data>,
    |builder: &mut TypedValueArrayBuilder, value: LiteralRef<'data>| {
        let result = builder.append_literal(value);
        match result {
            Err(LiteralEncodingError::ParsingError(_)) => builder.append_null(),
            Err(LiteralEncodingError::Arrow(arrow_error)) => Err(arrow_error),
            Ok(()) => Ok(()),
        }
    }
);
