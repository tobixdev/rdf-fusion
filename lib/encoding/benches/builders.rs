#![allow(clippy::panic)]

use codspeed_criterion_compat::{criterion_group, criterion_main, Criterion};
use rdf_fusion_encoding::plain_term::PlainTermArrayBuilder;
use rdf_fusion_encoding::sortable_term::SortableTermArrayBuilder;
use rdf_fusion_encoding::typed_value::TypedValueArrayBuilder;
use rdf_fusion_model::{NamedNodeRef, TermRef};
use std::hint::black_box;

/// These benchmarks measure instantiating [PlainTermArrayBuilder].
fn plain_term_builder_new(c: &mut Criterion) {
    c.bench_function("PlainTermArrayBuilder::new - Create empty builder", |b| {
        b.iter(|| black_box(PlainTermArrayBuilder::new(0)));
    });

    c.bench_function(
        "PlainTermArrayBuilder::new - Create record batch builder",
        |b| {
            b.iter(|| black_box(PlainTermArrayBuilder::new(8192)));
        },
    );
}

/// These benchmarks measure encoding quads and finishing the builder for [PlainTermArrayBuilder].
fn plain_term_builder_build_array(c: &mut Criterion) {
    c.bench_function(
        "PlainTermArrayBuilder::append_term - With empty builder",
        |b| {
            b.iter(|| {
                let mut builder = PlainTermArrayBuilder::new(0);
                for _ in 0..8192 {
                    builder.append_term(TermRef::NamedNode(NamedNodeRef::new_unchecked(
                        "http://example.com/test",
                    )))
                }
                let result = builder.finish();
                assert_eq!(result.len(), 8192);
            });
        },
    );
    c.bench_function(
        "PlainTermArrayBuilder::append_term - With pre-allocated builder",
        |b| {
            b.iter(|| {
                let mut builder = PlainTermArrayBuilder::new(8192);
                for _ in 0..8192 {
                    builder.append_term(TermRef::NamedNode(NamedNodeRef::new_unchecked(
                        "http://example.com/test",
                    )))
                }
                let result = builder.finish();
                assert_eq!(result.len(), 8192);
            });
        },
    );
}

/// These benchmarks measure instantiating [SortableTermArrayBuilder].
fn sortable_term_builder_new(c: &mut Criterion) {
    c.bench_function(
        "SortableTermArrayBuilder::new - Create empty builder",
        |b| {
            b.iter(|| black_box(SortableTermArrayBuilder::new(0)));
        },
    );
    c.bench_function(
        "SortableTermArrayBuilder::new - Create record batch builder",
        |b| {
            b.iter(|| black_box(SortableTermArrayBuilder::new(8192)));
        },
    );
}

/// These benchmarks measure instantiating [TypedValueArrayBuilder].
fn typed_value_builder_default(c: &mut Criterion) {
    c.bench_function(
        "TypedValueArrayBuilder::default - Create empty builder",
        |b| {
            b.iter(|| black_box(TypedValueArrayBuilder::default()));
        },
    );
}

criterion_group!(
    plain_term_builder,
    plain_term_builder_new,
    plain_term_builder_build_array
);
criterion_group!(sortable_term_builder, sortable_term_builder_new);
criterion_group!(typed_value_array_builder, typed_value_builder_default);
criterion_main!(
    plain_term_builder,
    sortable_term_builder,
    typed_value_array_builder
);
