#![cfg(test)]

use anyhow::Result;
use rdf_fusion_testsuite::check_testsuite;

#[tokio::test]
async fn sparql10_w3c_query_syntax_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-syntax.ttl",
        &[
            "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql3/manifest#syn-bad-26", /* tokenizer */
        ],
    ).await
}

#[tokio::test]
async fn sparql10_w3c_query_evaluation_testsuite() -> Result<()> {
    check_testsuite("https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl", &[
        // Testing equality of illegal literals ("xyz"^^<http://www.w3.org/2001/XMLSchema#integer>)
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-07",
        //Simple literal vs xsd:string. We apply RDF 1.1
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-08",
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-10",
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-11",
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-12",
        // We use XSD 1.1 equality on dates
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#date-2",
        // We choose to simplify first the nested group patterns in OPTIONAL
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional-filter/manifest#dawg-optional-filter-005-not-simplified",
        // This test relies on naive iteration on the input file
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/reduced/manifest#reduced-1",
        "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/reduced/manifest#reduced-2"
    ]).await
}

#[tokio::test]
async fn sparql11_query_w3c_evaluation_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
        &[],
    )
    .await
}

#[tokio::test]
#[ignore = "We do not support SPARQL 1.1 Federation yet"]
async fn sparql11_federation_w3c_evaluation_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-fed.ttl",
        &[
            // Problem during service evaluation order
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/service/manifest#service5",
        ],
    )
    .await
}

#[tokio::test]
#[ignore = "We do not support SPARQL 1.1 Update yet"]
async fn sparql11_update_w3c_evaluation_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-update.ttl",
        &[
            // We allow multiple INSERT DATA with the same blank nodes
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-update-1/manifest#test_54",
        ],
    ).await
}

#[tokio::test]
async fn sparql11_json_w3c_evaluation_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/json-res/manifest.ttl",
        &[],
    )
    .await
}

#[tokio::test]
async fn sparql11_tsv_w3c_evaluation_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/csv-tsv-res/manifest.ttl",
        &[
            // We do not run CSVResultFormatTest tests yet
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/csv-tsv-res/manifest#csv01",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/csv-tsv-res/manifest#csv02",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/csv-tsv-res/manifest#csv03",
        ],
    )
    .await
}

#[tokio::test]
async fn sparql12_w3c_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/sparql/sparql12/manifest.ttl",
        &[
            // Literal normalization
            "https://w3c.github.io/rdf-tests/sparql/sparql12/grouping#group01",
        ],
    )
    .await
}

#[tokio::test]
#[ignore = "We do not support SPARQL-star yet"]
async fn sparql_star_syntax_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-star/tests/sparql/syntax/manifest.ttl",
        &[],
    )
    .await
}

#[tokio::test]
#[ignore = "We do not support SPARQL-star yet"]
async fn sparql_star_eval_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-star/tests/sparql/eval/manifest.ttl",
        &[],
    )
    .await
}
