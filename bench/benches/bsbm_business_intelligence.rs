//! Runs certain queries from the BSBM benchmark suite as part of the regular benchmark suite.
//!
//! The particular instance of a query (they are generated randomly) is picked arbitrarily. If we
//! ever decide that queries in this file are not representative, we can easily change the query.
//!
//! The tests assume the presence of the benchmark data.

use codspeed_criterion_compat::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use rdf_fusion::io::RdfFormat;
use rdf_fusion::store::Store;
use rdf_fusion::{QueryOptions, QueryResults};
use std::fs;
use std::path::PathBuf;
use tokio::runtime::{Builder, Runtime};

fn bsbm_business_intelligence_q1(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_bsbm_1000()).unwrap();

    c.bench_function("BSBM Business Intelligence 1000 - Query 1", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = store
                .query_opt(
                    "
            prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
            prefix rev: <http://purl.org/stuff/rev#>

            Select ?productType ?reviewCount
            {
                {
                    Select ?productType (count(?review) As ?reviewCount)
                    {
                        ?productType a bsbm:ProductType .
                        ?product a ?productType .
                        ?product bsbm:producer ?producer .
                        ?producer bsbm:country <http://downlode.org/rdf/iso-3166/countries#AT> .
                        ?review bsbm:reviewFor ?product .
                        ?review rev:reviewer ?reviewer .
                        ?reviewer bsbm:country <http://downlode.org/rdf/iso-3166/countries#US> .
                    }
                    Group By ?productType
                }
            }
            Order By desc(?reviewCount) ?productType
            Limit 10
            ",
                    QueryOptions::default(),
                )
                .await
                .unwrap();
            assert_number_of_results(result, 10).await;
        });
    });
}

fn bsbm_business_intelligence_q2(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_bsbm_1000()).unwrap();

    c.bench_function("BSBM Business Intelligence 1000 - Query 2", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = store.query_opt("
            prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>

            SELECT ?otherProduct ?sameFeatures
            {
                ?otherProduct a bsbm:Product .
                FILTER(?otherProduct != <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer13/Product636>)
                {
                    SELECT ?otherProduct (count(?otherFeature) As ?sameFeatures)
                    {
                        <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer13/Product636> bsbm:productFeature ?feature .
                        ?otherProduct bsbm:productFeature ?otherFeature .
                        FILTER(?feature=?otherFeature)
                    }
                    Group By ?otherProduct
                }
            }
            Order By desc(?sameFeatures) ?otherProduct
            Limit 10
            ", QueryOptions::default()).await.unwrap();
            assert_number_of_results(result, 10).await;
        });
    });
}

fn bsbm_business_intelligence_q3(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_bsbm_1000()).unwrap();

    c.bench_function("BSBM Business Intelligence 1000 - Query 3", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = store.query_opt("
            prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
            prefix bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
            prefix rev: <http://purl.org/stuff/rev#>   prefix dc: <http://purl.org/dc/elements/1.1/>
            prefix xsd: <http://www.w3.org/2001/XMLSchema#>
            Select ?product (xsd:float(?monthCount)/?monthBeforeCount As ?ratio)
            {
                {
                    Select ?product (count(?review) As ?monthCount)
                    {
                        ?review bsbm:reviewFor ?product .
                        ?review dc:date ?date .
                        Filter(?date >= \"2007-10-10\"^^<http://www.w3.org/2001/XMLSchema#date> && ?date < \"2007-11-07\"^^<http://www.w3.org/2001/XMLSchema#date>)\
                    }
                    Group By ?product
                }
                {
                    Select ?product (count(?review) As ?monthBeforeCount)
                    {
                        ?review bsbm:reviewFor ?product .
                        ?review dc:date ?date .
                        Filter(?date >= \"2007-09-12\"^^<http://www.w3.org/2001/XMLSchema#date> && ?date < \"2007-10-10\"^^<http://www.w3.org/2001/XMLSchema#date>)\
                    }
                    Group By ?product
                    Having (count(?review)>0)
                }
            }
            Order By desc(xsd:float(?monthCount) / ?monthBeforeCount) ?product
            Limit 10
            ", QueryOptions::default()).await.unwrap();
            assert_number_of_results(result, 10).await;
        });
    });
}

fn bsbm_business_intelligence_q4(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_bsbm_1000()).unwrap();

    c.bench_function("BSBM Business Intelligence 1000 - Query 4", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = store.query_opt("
             prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
             prefix bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
             prefix xsd: <http://www.w3.org/2001/XMLSchema#>

             Select ?feature (?withFeaturePrice/?withoutFeaturePrice As ?priceRatio)
             {
                 {
                    Select ?feature (avg(xsd:float(xsd:string(?price))) As ?withFeaturePrice)
                    {
                        ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType3> ;
                            bsbm:productFeature ?feature .
                        ?offer bsbm:product ?product ;
                            bsbm:price ?price .
                    }
                    Group By ?feature
                }
                {
                    Select ?feature (avg(xsd:float(xsd:string(?price))) As ?withoutFeaturePrice)
                    {
                        {
                            Select distinct ?feature
                            {
                                ?p a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType3> ;
                                    bsbm:productFeature ?feature .
                            }
                        }
                        ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType3> .
                        ?offer bsbm:product ?product ;
                            bsbm:price ?price .
                        FILTER NOT EXISTS { ?product bsbm:productFeature ?feature }
                    }
                    Group By ?feature
                }
            }
            Order By desc(?withFeaturePrice/?withoutFeaturePrice) ?feature
            Limit 10
            ", QueryOptions::default()).await.unwrap();
            assert_number_of_results(result, 10).await;
        });
    });
}

fn bsbm_business_intelligence_q5(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_bsbm_1000()).unwrap();

    c.bench_function("BSBM Business Intelligence 1000 - Query 5", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = store.query_opt("
             prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
             prefix bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
             prefix rev: <http://purl.org/stuff/rev#>
             prefix xsd: <http://www.w3.org/2001/XMLSchema#>

             Select ?country ?product ?nrOfReviews ?avgPrice
             {
                {
                    Select ?country (max(?nrOfReviews) As ?maxReviews)
                    {
                        {
                            Select ?country ?product (count(?review) As ?nrOfReviews)
                            {
                                ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType111> .
                                ?review bsbm:reviewFor ?product ;
                                rev:reviewer ?reviewer .
                                ?reviewer bsbm:country ?country .
                            }
                            Group By ?country ?product
                        }
                    }
                    Group By ?country
                }
                {
                    Select ?country ?product (avg(xsd:float(xsd:string(?price))) As ?avgPrice)
                    {
                        ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType111> .
                        ?offer bsbm:product ?product .
                        ?offer bsbm:price ?price .
                    }
                    Group By ?country ?product
                }
                {
                    Select ?country ?product (count(?review) As ?nrOfReviews)
                    {
                        ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType111> .
                        ?review bsbm:reviewFor ?product .
                        ?review rev:reviewer ?reviewer .
                        ?reviewer bsbm:country ?country .
                    }
                    Group By ?country ?product
                }
                FILTER(?nrOfReviews=?maxReviews)
            }
            Order By desc(?nrOfReviews) ?country ?product
            ", QueryOptions::default()).await.unwrap();
            assert_number_of_results(result, 12).await;
        });
    });
}

fn bsbm_business_intelligence_q6(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_bsbm_1000()).unwrap();

    c.bench_function("BSBM Business Intelligence 1000 - Query 6", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = store.query_opt("
             prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
             prefix bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
             prefix rev: <http://purl.org/stuff/rev#>
             prefix xsd: <http://www.w3.org/2001/XMLSchema#>

             Select ?reviewer (avg(xsd:float(?score)) As ?reviewerAvgScore)
             {
                {
                    Select (avg(xsd:float(?score)) As ?avgScore)
                    {
                        ?product bsbm:producer <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Producer6> .
                        ?review bsbm:reviewFor ?product .
                        { ?review bsbm:rating1 ?score . } UNION
                        { ?review bsbm:rating2 ?score . } UNION
                        { ?review bsbm:rating3 ?score . } UNION
                        { ?review bsbm:rating4 ?score . }
                     }
                }
                ?product bsbm:producer <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/dataFromProducer6/Producer6> .
                ?review bsbm:reviewFor ?product .
                ?review rev:reviewer ?reviewer .
                { ?review bsbm:rating1 ?score . } UNION
                { ?review bsbm:rating2 ?score . } UNION
                { ?review bsbm:rating3 ?score . } UNION
                { ?review bsbm:rating4 ?score . }
            }
            Group By ?reviewer
            Having (avg(xsd:float(?score)) > min(?avgScore) * 1.5)
            ", QueryOptions::default()).await.unwrap();
            assert_number_of_results(result, 12).await;
        });
    });
}

fn bsbm_business_intelligence_q7(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_bsbm_1000()).unwrap();

    c.bench_function("BSBM Business Intelligence 1000 - Query 7", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = store.query_opt("
             prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
             prefix bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
             prefix xsd: <http://www.w3.org/2001/XMLSchema#>

             Select ?product
             {
                {
                    Select ?product
                    {
                        {
                            Select ?product (count(?offer) As ?offerCount)
                            {
                                ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType1> .
                                ?offer bsbm:product ?product .
                            }
                            Group By ?product
                        }
                    }
                    Order By desc(?offerCount)
                    Limit 1000
                }
                FILTER NOT EXISTS
                {
                    ?offer bsbm:product ?product .
                    ?offer bsbm:vendor ?vendor .
                    ?vendor bsbm:country ?country .
                    FILTER(?country=<http://downlode.org/rdf/iso-3166/countries#US>)
                }
            }
            ", QueryOptions::default()).await.unwrap();
            assert_number_of_results(result, 16).await;
        });
    });
}

fn bsbm_business_intelligence_q8(c: &mut Criterion) {
    let runtime = create_runtime();
    let store = runtime.block_on(load_bsbm_1000()).unwrap();

    c.bench_function("BSBM Business Intelligence 1000 - Query 8", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = store.query_opt("
             prefix bsbm: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/>
             prefix bsbm-inst: <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/>
             prefix xsd: <http://www.w3.org/2001/XMLSchema#>

             Select ?vendor (xsd:float(?belowAvg)/?offerCount As ?cheapExpensiveRatio)
             {
                {
                    Select ?vendor (count(?offer) As ?belowAvg)
                    {
                        {
                            ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType4> .
                            ?offer bsbm:product ?product .
                            ?offer bsbm:vendor ?vendor .
                            ?offer bsbm:price ?price .
                            {
                                Select ?product (avg(xsd:float(xsd:string(?price))) As ?avgPrice)
                                {
                                    ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType4> .
                                    ?offer bsbm:product ?product .
                                    ?offer bsbm:vendor ?vendor .
                                    ?offer bsbm:price ?price .
                                }
                                Group By ?product
                            }
                        } .
                        FILTER (xsd:float(xsd:string(?price)) < ?avgPrice)
                    }
                    Group By ?vendor
                }
                {
                    Select ?vendor (count(?offer) As ?offerCount)
                    {
                        ?product a <http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType4> .
                        ?offer bsbm:product ?product .
                        ?offer bsbm:vendor ?vendor .
                    }
                    Group By ?vendor
                }
            }
            Order by desc(xsd:float(?belowAvg)/?offerCount) ?vendor
            limit 10
            ", QueryOptions::default()).await.unwrap();
            assert_number_of_results(result, 10).await;
        });
    });
}

criterion_group!(
    bsbm_business_intelligence,
    bsbm_business_intelligence_q1,
    bsbm_business_intelligence_q2,
    bsbm_business_intelligence_q3,
    bsbm_business_intelligence_q4,
    bsbm_business_intelligence_q5,
    bsbm_business_intelligence_q6,
    bsbm_business_intelligence_q7,
    bsbm_business_intelligence_q8,
);
criterion_main!(bsbm_business_intelligence);

fn create_runtime() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

async fn load_bsbm_1000() -> anyhow::Result<Store> {
    let data_path = PathBuf::from("./data/bsbm-bi-1000/dataset.nt");
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
