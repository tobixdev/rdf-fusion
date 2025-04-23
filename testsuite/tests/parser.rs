#![cfg(test)]

use anyhow::Result;
use oxigraph_testsuite::check_testsuite;

// TODO: add support of language directions

#[tokio::test]
async fn rdf11_n_triples_w3c_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-triples/manifest.ttl",
        &[],
    )
    .await
}

#[tokio::test]
async fn rdf12_n_triples_syntax_w3c_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/syntax/manifest.ttl",
        &[
            "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/syntax#ntriples-base-1",
            "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/syntax#ntriples-base-2",
            "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/syntax#ntriples-star-1",
            "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/syntax#ntriples-star-2",
            "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/syntax#ntriples-star-3",
            "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/syntax#ntriples-star-bnode-1",
            "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/syntax#ntriples-star-nested-1",
            "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/syntax#ntriples-star-bad-quoted-1",
            "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/syntax#ntriples-star-bad-quoted-2",
            "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/syntax#ntriples-star-bad-quoted-3",
            "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/syntax#ntriples-star-bad-quoted-4",
        ],
    ).await
}

#[cfg(not(windows))] // Tests don't like git auto "\r\n" on Windows
#[tokio::test]
async fn rdf12_n_triples_c14n_w3c_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/c14n/manifest.ttl",
        &["https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-n-triples/c14n#dirlangtagged_string"],
    )
    .await
}

#[tokio::test]
async fn rdf11_n_quads_w3c_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-n-quads/manifest.ttl",
        &[],
    )
    .await
}

#[cfg(not(windows))] // Tests don't like git auto "\r\n" on Windows
#[tokio::test]
async fn rdf11_turtle_w3c_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-turtle/manifest.ttl",
        &[],
    )
    .await
}

#[tokio::test]
async fn rdf12_turtle_syntax_w3c_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-turtle/syntax/manifest.ttl",
        &[
            "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-turtle/syntax#nt-ttl-base-1",
            "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-turtle/syntax#nt-ttl-base-2",
        ],
    )
    .await
}

#[tokio::test]
async fn rdf12_turtle_eval_w3c_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-turtle/eval/manifest.ttl",
        &[],
    )
    .await
}

#[cfg(not(windows))] // Tests don't like git auto "\r\n" on Windows
#[tokio::test]
async fn rdf11_trig_w3c_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-trig/manifest.ttl",
        &[],
    )
    .await
}

#[tokio::test]
async fn rdf12_trig_syntax_w3c_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-trig/syntax/manifest.ttl",
        &[
            "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-trig/syntax#trig-base-1",
            "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-trig/syntax#trig-base-2",
        ],
    )
    .await
}

#[tokio::test]
async fn rdf12_trig_eval_w3c_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/rdf/rdf12/rdf-trig/eval/manifest.ttl",
        &[],
    )
    .await
}

#[tokio::test]
async fn rdf11_xml_w3c_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/rdf-tests/rdf/rdf11/rdf-xml/manifest.ttl",
        &[],
    )
    .await
}

#[tokio::test]
async fn n3_parser_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/N3/tests/N3Tests/manifest-parser.ttl",
        &[],
    )
    .await
}

#[tokio::test]
async fn n3_extended_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/N3/tests/N3Tests/manifest-extended.ttl",
        &[],
    )
    .await
}

#[cfg(not(windows))] // Tests don't like git auto "\r\n" on Windows
#[tokio::test]
async fn n3_turtle_testsuite() -> Result<()> {
    check_testsuite(
        "https://w3c.github.io/N3/tests/TurtleTests/manifest.ttl",
        &[],
    )
    .await
}
