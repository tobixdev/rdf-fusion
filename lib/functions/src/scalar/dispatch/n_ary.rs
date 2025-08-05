use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_common::{DFResult, ObjectId};
use rdf_fusion_encoding::object_id::{
    DefaultObjectIdDecoder, ObjectIdArrayBuilder, ObjectIdEncoding,
};
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::plain_term::decoders::DefaultPlainTermDecoder;
use rdf_fusion_encoding::plain_term::encoders::DefaultPlainTermEncoder;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_encoding::typed_value::decoders::DefaultTypedValueDecoder;
use rdf_fusion_encoding::typed_value::encoders::DefaultTypedValueEncoder;
use rdf_fusion_encoding::{EncodingArray, EncodingDatum, TermEncoder};
use rdf_fusion_model::{TermRef, ThinResult, TypedValue, TypedValueRef};

pub fn dispatch_n_ary_plain_term(
    args: &[EncodingDatum<PlainTermEncoding>],
    number_of_rows: usize,
    op: impl for<'a> Fn(&[TermRef<'a>]) -> ThinResult<TermRef<'a>>,
    error_op: impl for<'a> Fn(&[ThinResult<TermRef<'a>>]) -> ThinResult<TermRef<'a>>,
) -> DFResult<ColumnarValue> {
    if args.is_empty() {
        let results = (0..number_of_rows).map(|_| op(&[]));
        let result = DefaultPlainTermEncoder::encode_terms(results)?;
        return Ok(ColumnarValue::Array(result.into_array()));
    }

    let mut iters = Vec::new();
    for arg in args {
        iters.push(arg.term_iter::<DefaultPlainTermDecoder>());
    }

    let results = multi_zip(iters).map(|args| {
        if args.iter().all(Result::is_ok) {
            let args = args.into_iter().map(|arg| arg.unwrap()).collect::<Vec<_>>();
            op(args.as_slice())
        } else {
            error_op(args.as_slice())
        }
    });
    let result = DefaultPlainTermEncoder::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

pub fn dispatch_n_ary_typed_value(
    args: &[EncodingDatum<TypedValueEncoding>],
    number_of_rows: usize,
    op: impl for<'a> Fn(&[TypedValueRef<'a>]) -> ThinResult<TypedValueRef<'a>>,
    error_op: impl for<'a> Fn(
        &[ThinResult<TypedValueRef<'a>>],
    ) -> ThinResult<TypedValueRef<'a>>,
) -> DFResult<ColumnarValue> {
    if args.is_empty() {
        let results = (0..number_of_rows).map(|_| op(&[]));
        let result = DefaultTypedValueEncoder::encode_terms(results)?;
        return Ok(ColumnarValue::Array(result.into_array()));
    }

    let mut iters = Vec::new();
    for arg in args {
        iters.push(arg.term_iter::<DefaultTypedValueDecoder>());
    }

    let results = multi_zip(iters).map(|args| {
        if args.iter().all(Result::is_ok) {
            let args = args.into_iter().map(|arg| arg.unwrap()).collect::<Vec<_>>();
            op(args.as_slice())
        } else {
            error_op(args.as_slice())
        }
    });
    let result = DefaultTypedValueEncoder::encode_terms(results)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

pub fn dispatch_n_ary_owned_typed_value(
    args: &[EncodingDatum<TypedValueEncoding>],
    number_of_rows: usize,
    op: impl for<'a> Fn(&[TypedValueRef<'a>]) -> ThinResult<TypedValue>,
    error_op: impl for<'a> Fn(&[ThinResult<TypedValueRef<'a>>]) -> ThinResult<TypedValue>,
) -> DFResult<ColumnarValue> {
    if args.is_empty() {
        let results = (0..number_of_rows).map(|_| op(&[])).collect::<Vec<_>>();
        let result_refs = results.iter().map(|r| match r {
            Ok(res) => Ok(res.as_ref()),
            Err(err) => Err(*err),
        });
        let result = DefaultTypedValueEncoder::encode_terms(result_refs)?;
        return Ok(ColumnarValue::Array(result.into_array()));
    }

    let mut iters = Vec::new();
    for arg in args {
        iters.push(arg.term_iter::<DefaultTypedValueDecoder>());
    }

    let results = multi_zip(iters)
        .map(|args| {
            if args.iter().all(Result::is_ok) {
                let args = args.into_iter().map(|arg| arg.unwrap()).collect::<Vec<_>>();
                op(args.as_slice())
            } else {
                error_op(args.as_slice())
            }
        })
        .collect::<Vec<_>>();
    let result_refs = results.iter().map(|r| match r {
        Ok(res) => Ok(res.as_ref()),
        Err(err) => Err(*err),
    });
    let result = DefaultTypedValueEncoder::encode_terms(result_refs)?;
    Ok(ColumnarValue::Array(result.into_array()))
}

pub fn dispatch_n_ary_object_id(
    encoding: &ObjectIdEncoding,
    args: &[EncodingDatum<ObjectIdEncoding>],
    number_of_rows: usize,
    op: impl Fn(&[ObjectId]) -> ThinResult<ObjectId>,
    error_op: impl Fn(&[ThinResult<ObjectId>]) -> ThinResult<ObjectId>,
) -> DFResult<ColumnarValue> {
    if args.is_empty() {
        let mut builder = ObjectIdArrayBuilder::new(encoding.clone());
        for result in (0..number_of_rows).map(|_| op(&[]).ok()) {
            builder.append_object_id_opt(result);
        }
        return Ok(ColumnarValue::Array(builder.finish().into_array()));
    }

    let mut iters = Vec::new();
    for arg in args {
        iters.push(arg.term_iter::<DefaultObjectIdDecoder>());
    }

    let results = multi_zip(iters)
        .map(|args| {
            if args.iter().all(Result::is_ok) {
                let args = args.into_iter().map(|arg| arg.unwrap()).collect::<Vec<_>>();
                op(args.as_slice())
            } else {
                error_op(args.as_slice())
            }
        })
        .map(Result::ok);

    let mut builder = ObjectIdArrayBuilder::new(encoding.clone());
    for result in results {
        builder.append_object_id_opt(result);
    }
    Ok(ColumnarValue::Array(builder.finish().into_array()))
}

fn multi_zip<I, T>(mut iterators: Vec<I>) -> impl Iterator<Item = Vec<T>>
where
    I: Iterator<Item = T>,
{
    std::iter::from_fn(move || {
        let mut items = Vec::with_capacity(iterators.len());
        for iter in &mut iterators {
            match iter.next() {
                Some(item) => items.push(item),
                None => return None, // Stop if any iterator is exhausted
            }
        }

        if items.is_empty() { None } else { Some(items) }
    })
}
