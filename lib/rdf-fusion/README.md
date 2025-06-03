RdfFusion
========

TBD


Here is an example of using RdfFusion:
```rust
use std::error::Error;
use rdf_fusion::store::Store;
use rdf_fusion::model::*;
use rdf_fusion::sparql::QueryResults;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let store = Store::new();

    // insertion
    let ex = NamedNode::new("http://example.com")?;
    let quad = Quad::new(ex.clone(), ex.clone(), ex.clone(), GraphName::DefaultGraph);
    store.insert(&quad).await?;

    // quad filter
    let results = store
        .quads_for_pattern(Some(ex.as_ref().into()), None, None, None).await?
        .try_collect_to_vec().await?;
    assert_eq!(vec![quad], results);

    // SPARQL query
    if let QueryResults::Solutions(mut solutions) = store.query("SELECT ?s WHERE { ?s ?p ?o }").await? {
        assert_eq!(solutions.next().await.unwrap().unwrap().get("s"), Some(&ex.into()));
    }
    
    Ok(())
}
```
