//! Runs certain queries from the BSBM benchmark suite as part of the regular benchmark suite.
//!
//! The particular instance of a query (they are generated randomly) is picked arbitrarily. If we
//! ever decide that queries in this file are not representative, we can easily change the query.
//!
//! The tests assume the presence of the benchmark data.

use codspeed_criterion_compat::{criterion_group, criterion_main, Criterion};
use futures::StreamExt;
use rdf_fusion::io::RdfFormat;
use rdf_fusion::store::Store;
use rdf_fusion::{QueryOptions, QueryResults};
use std::fs;
use std::path::PathBuf;
use tokio::runtime::{Builder, Runtime};

fn bsbm_explore_q1(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_bsbm_1000()).unwrap();

    c.bench_function("BSBM Explore 1000 - Query 1", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = store.query_opt("
            PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
            PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

            SELECT DISTINCT ?product ?label WHERE {
                ?product rdfs:label ?label .
                ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType66> .
                ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature3> .
                ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature1967> .
                ?product bsbm:productPropertyNumeric1 ?value1 .

                FILTER (?value1 > 136)
            } ORDER BY ?label LIMIT 10
            ", QueryOptions::default()).await.unwrap();
            assert_number_of_results(result, 0).await;
        });
    });
}

fn bsbm_explore_q2(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_bsbm_1000()).unwrap();

    c.bench_function("BSBM Explore 1000 - Query 2", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = store.query_opt("
            PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
            PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX dc: <http://purl.org/dc/elements/1.1/>

            SELECT ?label ?comment ?producer ?productFeature ?propertyTextual1 ?propertyTextual2 ?propertyTextual3  ?propertyNumeric1 ?propertyNumeric2 ?propertyTextual4 ?propertyTextual5 ?propertyNumeric4
            WHERE {
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> rdfs:label ?label .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> rdfs:comment ?comment .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:producer ?p .
                ?p rdfs:label ?producer .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> dc:publisher ?p .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productFeature ?f .
                ?f rdfs:label ?productFeature .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyTextual1 ?propertyTextual1 .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyTextual2 ?propertyTextual2 .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyTextual3 ?propertyTextual3 .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyNumeric1 ?propertyNumeric1 .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyNumeric2 ?propertyNumeric2 .
                OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyTextual4 ?propertyTextual4 }
                OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyTextual5 ?propertyTextual5 }
                OPTIONAL { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Product272> bsbm:productPropertyNumeric4 ?propertyNumeric4 }
            }
            ", QueryOptions::default()).await.unwrap();
            assert_number_of_results(result, 0).await;
        });
    });
}

fn bsbm_explore_q3(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_bsbm_1000()).unwrap();

    c.bench_function("BSBM Explore 1000 - Query 3", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = store.query_opt("
            PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
            PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

            SELECT ?product ?label WHERE {
                ?product rdfs:label ?label .
                ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType87> .
                ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature541> .
                ?product bsbm:productPropertyNumeric1 ?p1 .
                FILTER ( ?p1 > 156 )

                ?product bsbm:productPropertyNumeric3 ?p3 .
                FILTER (?p3 < 152 )

                OPTIONAL {
                    ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature553> .
                    ?product rdfs:label ?testVar
                }
                FILTER (!bound(?testVar))
             }
             ORDER BY ?label
             LIMIT 10
            ", QueryOptions::default()).await.unwrap();
            assert_number_of_results(result, 0).await;
        });
    });
}

fn bsbm_explore_q4(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_bsbm_1000()).unwrap();

    c.bench_function("BSBM Explore 1000 - Query 4", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = store.query_opt("
            PREFIX bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
            PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

            SELECT DISTINCT ?product ?label ?propertyTextual
            WHERE {
                {
                    ?product rdfs:label ?label .
                    ?product rdf:type <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType138> .
                    ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature4305> .
                    ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature1427> .
                    ?product bsbm:productPropertyTextual1 ?propertyTextual .
                    ?product bsbm:productPropertyNumeric1 ?p1 .
                    FILTER ( ?p1 > 457 )
                }
                UNION
                {
                    ?product rdfs:label ?label .
                    ?product rdf:type <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType138> .
                    ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature4305> .
                    ?product bsbm:productFeature <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature1444> .
                    ?product bsbm:productPropertyTextual1 ?propertyTextual .
                    ?product bsbm:productPropertyNumeric2 ?p2 .
                    FILTER ( ?p2> 488 )
                }
            }
            ORDER BY ?label
            OFFSET 5
            LIMIT 10
            ", QueryOptions::default()).await.unwrap();
            assert_number_of_results(result, 0).await;
        });
    });
}

// fn bsbm_explore_q5(c: &mut Criterion) {
//     let runtime = create_runtime();
//     let store = runtime.block_on(load_bsbm_1000()).unwrap();
//
//     c.bench_function("BSBM Explore 1000 - Query 5", |b| {
//         b.to_async(&runtime).iter(|| async {
//             let result = store.query_opt("
//             PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
//             PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
//             PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
//
//             SELECT DISTINCT ?product ?productLabel WHERE {
//                 ?product rdfs:label ?productLabel .
//                 FILTER (<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer19/Product890> != ?product)
//
//                 <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer19/Product890> bsbm:productFeature ?prodFeature .
//                 ?product bsbm:productFeature ?prodFeature .
//                 <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer19/Product890> bsbm:productPropertyNumeric1 ?origProperty1 .
//                 ?product bsbm:productPropertyNumeric1 ?simProperty1 .
//                 FILTER (?simProperty1 < (?origProperty1 + 120) && ?simProperty1 > (?origProperty1 - 120))
//
//                 <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer19/Product890> bsbm:productPropertyNumeric2 ?origProperty2 .
//                 ?product bsbm:productPropertyNumeric2 ?simProperty2 .
//                 FILTER (?simProperty2 < (?origProperty2 + 170) && ?simProperty2 > (?origProperty2 - 170))
//             }
//             ORDER BY ?productLabel
//             LIMIT 5
//             ", QueryOptions::default()).await.unwrap();
//             assert_number_of_results(result, 0).await;
//         });
//     });
// }

fn bsbm_explore_q7(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_bsbm_1000()).unwrap();

    c.bench_function("BSBM Explore 1000 - Query 7", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = store.query_opt("
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rev: <http://purl.org/stuff/rev#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
            PREFIX dc: <http://purl.org/dc/elements/1.1/>

            SELECT ?productLabel ?offer ?price ?vendor ?vendorTitle ?review ?revTitle ?reviewer ?revName ?rating1 ?rating2
            WHERE {
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer17/Product801> rdfs:label ?productLabel .
                OPTIONAL {
                    ?offer bsbm:product <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer17/Product801> .
                    ?offer bsbm:price ?price .
                    ?offer bsbm:vendor ?vendor .
                    ?vendor rdfs:label ?vendorTitle .
                    ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#DE> .
                    ?offer dc:publisher ?vendor .
                    ?offer bsbm:validTo ?date .
                    FILTER (?date > \"2008-06-20T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> )
                }
                OPTIONAL {
                	?review bsbm:reviewFor <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer17/Product801> .
                	?review rev:reviewer ?reviewer .
                	?reviewer foaf:name ?revName .
                	?review dc:title ?revTitle .
                	OPTIONAL {
                	    ?review bsbm:rating1 ?rating1 .
                    }
                    OPTIONAL {
                        ?review bsbm:rating2 ?rating2 .
                    }
                }
            }
            ", QueryOptions::default()).await.unwrap();
            assert_number_of_results(result, 4).await;
        });
    });
}

fn bsbm_explore_q8(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_bsbm_1000()).unwrap();

    c.bench_function("BSBM Explore 1000 - Query 8", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = store.query_opt("
            PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
            PREFIX dc: <http://purl.org/dc/elements/1.1/>
            PREFIX rev: <http://purl.org/stuff/rev#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>

            SELECT ?title ?text ?reviewDate ?reviewer ?reviewerName ?rating1 ?rating2 ?rating3 ?rating4
            WHERE {
                ?review bsbm:reviewFor <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer12/Product578> .
                ?review dc:title ?title .
                ?review rev:text ?text .
                FILTER langMatches( lang(?text), \"EN\" )

                ?review bsbm:reviewDate ?reviewDate .
                ?review rev:reviewer ?reviewer .
                ?reviewer foaf:name ?reviewerName .

                OPTIONAL { ?review bsbm:rating1 ?rating1 . }
                OPTIONAL { ?review bsbm:rating2 ?rating2 . }
                OPTIONAL { ?review bsbm:rating3 ?rating3 . }
                OPTIONAL { ?review bsbm:rating4 ?rating4 . }
            }
            ORDER BY DESC(?reviewDate)
            LIMIT 20
            ", QueryOptions::default()).await.unwrap();
            assert_number_of_results(result, 9).await;
        });
    });
}

fn bsbm_explore_q10(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_bsbm_1000()).unwrap();

    c.bench_function("BSBM Explore 1000 - Query 10", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = store.query_opt("
            PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
            PREFIX dc: <http://purl.org/dc/elements/1.1/>

            SELECT DISTINCT ?offer ?price
            WHERE {
                ?offer bsbm:product <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer7/Product296> .
                ?offer bsbm:vendor ?vendor .
                ?offer dc:publisher ?vendor .
                ?vendor bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> .
                ?offer bsbm:deliveryDays ?deliveryDays .
                FILTER (?deliveryDays <= 3)

                ?offer bsbm:price ?price .
                ?offer bsbm:validTo ?date .
                FILTER (?date > \"2008-06-20T00:00:00\"^^<http://www.w3.org/2001/XMLSchema#dateTime> )
            }
            ORDER BY xsd:double(str(?price))
            LIMIT 10
            ", QueryOptions::default()).await.unwrap();
            assert_number_of_results(result, 3).await;
        });
    });
}

fn bsbm_explore_q11(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_bsbm_1000()).unwrap();

    c.bench_function("BSBM Explore 1000 - Query 11", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = store.query_opt("
            SELECT ?property ?hasValue ?isValueOf
            WHERE {
                { <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1250> ?property ?hasValue }
                UNION
                { ?isValueOf ?property <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor1/Offer1250> }
            }
            ", QueryOptions::default()).await.unwrap();
            assert_number_of_results(result, 10).await;
        });
    });
}

fn bsbm_explore_q12(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_bsbm_1000()).unwrap();

    c.bench_function("BSBM Explore 1000 - Query 12", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = store.query_opt("
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX rev: <http://purl.org/stuff/rev#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
            PREFIX bsbm-export: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/export/>
            PREFIX dc: <http://purl.org/dc/elements/1.1/>

            CONSTRUCT {
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm-export:product ?productURI .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm-export:productlabel ?productlabel .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm-export:vendor ?vendorname .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm-export:vendorhomepage ?vendorhomepage .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm-export:offerURL ?offerURL .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm-export:price ?price .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm-export:deliveryDays ?deliveryDays .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm-export:validuntil ?validTo
            }
            WHERE {
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm:product ?productURI .
                ?productURI rdfs:label ?productlabel .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm:vendor ?vendorURI .
                ?vendorURI rdfs:label ?vendorname .
                ?vendorURI foaf:homepage ?vendorhomepage .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm:offerWebpage ?offerURL .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm:price ?price .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm:deliveryDays ?deliveryDays .
                <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromVendor5/Offer9035> bsbm:validTo ?validTo
            }
            ", QueryOptions::default()).await.unwrap();
            assert_number_of_results(result, 8).await;
        });
    });
}

criterion_group!(
    bsbm_explore,
    bsbm_explore_q1,
    bsbm_explore_q2,
    bsbm_explore_q3,
    bsbm_explore_q4,
    // bsbm_explore_q5, TODO investigate OOM
    bsbm_explore_q7,
    bsbm_explore_q8,
    bsbm_explore_q10,
    bsbm_explore_q11,
    bsbm_explore_q12
);
criterion_main!(bsbm_explore);

fn create_runtime() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

async fn load_bsbm_1000() -> anyhow::Result<Store> {
    let data_path = PathBuf::from("./data/dataset-1000.nt");
    let data = fs::read(data_path)?;
    let memory_store = Store::new();
    memory_store
        .load_from_reader(RdfFormat::NTriples, data.as_slice())
        .await?;
    Ok(memory_store)
}

async fn assert_number_of_results(result: QueryResults, n: usize) {
    match result {
        QueryResults::Solutions(mut solutions) => {
            let mut count = 0;
            while let Some(sol) = solutions.next().await {
                sol.unwrap();
                count += 1;
            }
            assert_eq!(count, n);
        }
        QueryResults::Graph(mut triples) => {
            let mut count = 0;
            while let Some(sol) = triples.next().await {
                sol.unwrap();
                count += 1;
            }
            assert_eq!(count, n);
        }
        _ => panic!("Unexpected QueryResults"),
    }
}
