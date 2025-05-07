use crate::value_encoding::decoders::DefaultTermValueDecoder;
use crate::TermDecoder;
use crate::TermEncoder;
use crate::TermEncoding;
use crate::TermValueEncoding;
use graphfusion_model::{
    Boolean, DateTime, Integer, NamedNodeRef, Numeric, SimpleLiteralRef, StringLiteralRef,
    ThinError,
};
use graphfusion_model::{TermValueRef, ThinResult};

#[macro_export]
macro_rules! make_simple_term_value_decoder {
    ($STRUCT_NAME: ident, $VALUE_TYPE: ty, $MAPPING_EXPRESSION: expr) => {
        pub struct $STRUCT_NAME {}

        impl TermDecoder<TermValueEncoding> for $STRUCT_NAME {
            type Term<'data> = $VALUE_TYPE;

            fn decode_terms(
                array: &<TermValueEncoding as TermEncoding>::Array,
            ) -> impl Iterator<Item = ThinResult<Self::Term<'_>>> {
                DefaultTermValueDecoder::decode_terms(array)
                    .map(|res| res.and_then($MAPPING_EXPRESSION))
            }

            fn decode_term(
                scalar: &<TermValueEncoding as TermEncoding>::Scalar,
            ) -> ThinResult<Self::Term<'_>> {
                todo!()
            }
        }
    };
}

make_simple_term_value_decoder!(
    NamedNodeRefTermValueDecoder,
    NamedNodeRef<'data>,
    |value: TermValueRef<'_>| {
        match value {
            TermValueRef::NamedNode(value) => Ok(value),
            _ => ThinError::expected(),
        }
    }
);

make_simple_term_value_decoder!(BooleanTermValueDecoder, Boolean, |value: TermValueRef<
    '_,
>| {
    match value {
        TermValueRef::BooleanLiteral(value) => Ok(value),
        _ => ThinError::expected(),
    }
});

make_simple_term_value_decoder!(NumericTermValueDecoder, Numeric, |value: TermValueRef<
    '_,
>| {
    match value {
        TermValueRef::NumericLiteral(value) => Ok(value),
        _ => ThinError::expected(),
    }
});

make_simple_term_value_decoder!(IntegerTermValueDecoder, Integer, |value: TermValueRef<
    '_,
>| {
    match value {
        TermValueRef::NumericLiteral(Numeric::Integer(value)) => Ok(value),
        _ => ThinError::expected(),
    }
});

make_simple_term_value_decoder!(
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRef<'data>,
    |value: TermValueRef<'_>| {
        match value {
            TermValueRef::SimpleLiteral(value) => Ok(value),
            _ => ThinError::expected(),
        }
    }
);

make_simple_term_value_decoder!(
    StringLiteralRefTermValueDecoder,
    StringLiteralRef<'data>,
    |value: TermValueRef<'_>| {
        match value {
            TermValueRef::SimpleLiteral(value) => Ok(StringLiteralRef(value.value, None)),
            TermValueRef::LanguageStringLiteral(value) => {
                Ok(StringLiteralRef(value.value, Some(value.language)))
            }
            _ => ThinError::expected(),
        }
    }
);

make_simple_term_value_decoder!(DateTimeTermValueDecoder, DateTime, |value: TermValueRef<
    '_,
>| {
    match value {
        TermValueRef::DateTimeLiteral(value) => Ok(value),
        _ => ThinError::expected(),
    }
});
