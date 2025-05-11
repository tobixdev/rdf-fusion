use crate::typed_value::decoders::DefaultTypedValueDecoder;
use crate::TermDecoder;
use crate::TermEncoding;
use crate::TypedValueEncoding;
use graphfusion_model::{
    Boolean, DateTime, Integer, NamedNodeRef, Numeric, SimpleLiteralRef, StringLiteralRef,
    ThinError,
};
use graphfusion_model::{ThinResult, TypedValueRef};

#[macro_export]
macro_rules! make_simple_term_value_decoder {
    ($STRUCT_NAME: ident, $VALUE_TYPE: ty, $MAPPING_EXPRESSION: expr) => {
        #[derive(Debug)]
        pub struct $STRUCT_NAME {}

        impl TermDecoder<TypedValueEncoding> for $STRUCT_NAME {
            type Term<'data> = $VALUE_TYPE;

            fn decode_terms(
                array: &<TypedValueEncoding as TermEncoding>::Array,
            ) -> impl Iterator<Item = ThinResult<Self::Term<'_>>> {
                DefaultTypedValueDecoder::decode_terms(array)
                    .map(|res| res.and_then($MAPPING_EXPRESSION))
            }

            fn decode_term(
                scalar: &<TypedValueEncoding as TermEncoding>::Scalar,
            ) -> ThinResult<Self::Term<'_>> {
                todo!()
            }
        }
    };
}

make_simple_term_value_decoder!(
    NamedNodeRefTermValueDecoder,
    NamedNodeRef<'data>,
    |value: TypedValueRef<'_>| {
        match value {
            TypedValueRef::NamedNode(value) => Ok(value),
            _ => ThinError::expected(),
        }
    }
);

make_simple_term_value_decoder!(BooleanTermValueDecoder, Boolean, |value: TypedValueRef<
    '_,
>| {
    match value {
        TypedValueRef::BooleanLiteral(value) => Ok(value),
        _ => ThinError::expected(),
    }
});

make_simple_term_value_decoder!(NumericTermValueDecoder, Numeric, |value: TypedValueRef<
    '_,
>| {
    match value {
        TypedValueRef::NumericLiteral(value) => Ok(value),
        _ => ThinError::expected(),
    }
});

make_simple_term_value_decoder!(IntegerTermValueDecoder, Integer, |value: TypedValueRef<
    '_,
>| {
    match value {
        TypedValueRef::NumericLiteral(Numeric::Integer(value)) => Ok(value),
        _ => ThinError::expected(),
    }
});

make_simple_term_value_decoder!(
    SimpleLiteralRefTermValueDecoder,
    SimpleLiteralRef<'data>,
    |value: TypedValueRef<'_>| {
        match value {
            TypedValueRef::SimpleLiteral(value) => Ok(value),
            _ => ThinError::expected(),
        }
    }
);

make_simple_term_value_decoder!(
    StringLiteralRefTermValueDecoder,
    StringLiteralRef<'data>,
    |value: TypedValueRef<'_>| {
        match value {
            TypedValueRef::SimpleLiteral(value) => Ok(StringLiteralRef(value.value, None)),
            TypedValueRef::LanguageStringLiteral(value) => {
                Ok(StringLiteralRef(value.value, Some(value.language)))
            }
            _ => ThinError::expected(),
        }
    }
);

make_simple_term_value_decoder!(
    DateTimeTermValueDecoder,
    DateTime,
    |value: TypedValueRef<'_>| {
        match value {
            TypedValueRef::DateTimeLiteral(value) => Ok(value),
            _ => ThinError::expected(),
        }
    }
);
