use crate::value_encoding::TermValueArrayBuilder;

use crate::DFResult;
use crate::TermEncoder;
use crate::TermEncoding;
use crate::TermValueEncoding;
use datafusion::common::exec_err;
use graphfusion_model::{BlankNodeRef, NamedNodeRef, Numeric, SimpleLiteralRef, ThinResult};
use graphfusion_model::{BlankNode, Double, NamedNode};
use graphfusion_model::{
    Boolean, DateTime, DayTimeDuration, Decimal, Float, Int, Integer, OwnedStringLiteral, ThinError,
};

#[macro_export]
macro_rules! make_simple_term_value_encoder {
    ($STRUCT_NAME: ident, $VALUE_TYPE: ty, $BUILDER_INVOCATION: expr) => {
        pub struct $STRUCT_NAME {}

        impl TermEncoder<TermValueEncoding> for $STRUCT_NAME {
            type Term<'data> = $VALUE_TYPE;

            fn encode_terms<'data>(
                terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
            ) -> DFResult<<TermValueEncoding as TermEncoding>::Array> {
                let mut builder = TermValueArrayBuilder::default();
                for term_result in terms {
                    match term_result {
                        Ok(value) => $BUILDER_INVOCATION(&mut builder, value)?,
                        Err(ThinError::Expected) => builder.append_null()?,
                        Err(ThinError::InternalError(cause)) => {
                            return exec_err!("Internal error during RDF operation: {cause}")
                        }
                    }
                }
                TermValueEncoding::try_new_array(builder.finish())
            }

            fn encode_term(
                term: ThinResult<Self::Term<'_>>,
            ) -> DFResult<<TermValueEncoding as TermEncoding>::Scalar> {
                todo!()
            }
        }
    };
}

make_simple_term_value_encoder!(
    NamedNodeTermValueEncoder,
    NamedNode,
    |builder: &mut TermValueArrayBuilder, value: NamedNode| {
        builder.append_named_node(value.as_ref())
    }
);
make_simple_term_value_encoder!(
    NamedNodeRefTermValueEncoder,
    NamedNodeRef<'data>,
    |builder: &mut TermValueArrayBuilder, value: NamedNodeRef<'data>| {
        builder.append_named_node(value)
    }
);
make_simple_term_value_encoder!(
    BlankNodeTermValueEncoder,
    BlankNode,
    |builder: &mut TermValueArrayBuilder, value: BlankNode| {
        builder.append_blank_node(value.as_ref())
    }
);
make_simple_term_value_encoder!(
    BlankNodeRefTermValueEncoder,
    BlankNodeRef<'data>,
    |builder: &mut TermValueArrayBuilder, value: BlankNodeRef<'data>| {
        builder.append_blank_node(value)
    }
);
make_simple_term_value_encoder!(
    BooleanTermValueEncoder,
    Boolean,
    |builder: &mut TermValueArrayBuilder, value: Boolean| { builder.append_boolean(value) }
);
make_simple_term_value_encoder!(
    SimpleLiteralRefTermValueEncoder,
    SimpleLiteralRef<'data>,
    |builder: &mut TermValueArrayBuilder, value: SimpleLiteralRef<'data>| {
        builder.append_string(value.value, None)
    }
);
make_simple_term_value_encoder!(
    OwnedStringLiteralTermValueEncoder,
    OwnedStringLiteral,
    |builder: &mut TermValueArrayBuilder, value: OwnedStringLiteral| {
        builder.append_string(value.0.as_str(), value.1.as_deref())
    }
);
make_simple_term_value_encoder!(
    IntTermValueEncoder,
    Int,
    |builder: &mut TermValueArrayBuilder, value: Int| { builder.append_int(value) }
);
make_simple_term_value_encoder!(
    IntegerTermValueEncoder,
    Integer,
    |builder: &mut TermValueArrayBuilder, value: Integer| { builder.append_integer(value) }
);
make_simple_term_value_encoder!(
    FloatTermValueEncoder,
    Float,
    |builder: &mut TermValueArrayBuilder, value: Float| { builder.append_float(value) }
);
make_simple_term_value_encoder!(
    DoubleTermValueEncoder,
    Double,
    |builder: &mut TermValueArrayBuilder, value: Double| { builder.append_double(value) }
);
make_simple_term_value_encoder!(
    DecimalTermValueEncoder,
    Decimal,
    |builder: &mut TermValueArrayBuilder, value: Decimal| { builder.append_decimal(value) }
);
make_simple_term_value_encoder!(
    NumericTermValueEncoder,
    Numeric,
    |builder: &mut TermValueArrayBuilder, value: Numeric| { builder.append_numeric(value) }
);
make_simple_term_value_encoder!(
    DateTimeTermValueEncoder,
    DateTime,
    |builder: &mut TermValueArrayBuilder, value: DateTime| { builder.append_date_time(value) }
);
make_simple_term_value_encoder!(
    DayTimeDurationTermValueEncoder,
    DayTimeDuration,
    |builder: &mut TermValueArrayBuilder, value: DayTimeDuration| {
        builder.append_duration(None, Some(value))
    }
);
