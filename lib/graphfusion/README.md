GraphFusion
========

TBD

```rust
use graphfusion::store::Store;
use graphfusion::model::*;
use graphfusion::sparql::QueryResults;
use futures::StreamExt;

let store = Store::new();

// insertion
let ex = NamedNode::new("http://example.com").unwrap();
let quad = Quad::new(ex.clone(), ex.clone(), ex.clone(), GraphName::DefaultGraph);
store.insert(&quad).unwrap();

// quad filter
let results = store.quads_for_pattern(Some(ex.as_ref().into()), None, None, None).collect::<Result<Vec<Quad>, _ > > ().unwrap();
assert_eq!(vec![quad], results);

// SPARQL query
if let QueryResults::Solutions(mut solutions) = store.query("SELECT ?s WHERE { ?s ?p ?o }").unwrap() {
    assert_eq!(solutions.next().await.unwrap().unwrap().get("s"), Some( & ex.into()));
}
```