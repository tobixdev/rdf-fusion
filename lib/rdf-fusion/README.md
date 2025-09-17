RDF Fusion
========

RDF Fusion is an experimental columnar [SPARQL](https://www.w3.org/TR/sparql11-overview/) engine.
It is based on [Apache DataFusion](https://datafusion.apache.org/), an extensible query engine that
uses [Apache Arrow](https://arrow.apache.org/) as its in-memory data format.

# Using RDF Fusion

RDF Fusion can currently be used in two modes: via a convenient `Store` API or as a "library" for DataFusion.

## Store API

The `Store` API provides high-level methods for interacting with the database (e.g., inserting, querying).
Users that want to *use* RDF Fusion are advised to use this API.
Note that while the `Store` API is similar to [Oxigraph](https://github.com/oxigraph/oxigraph)'s `Store` we do not
strive for full
compatibility, as some aspects of RDF Fusion work fundamentally differently (e.g., `async` methods).

The `Store` API supports extending SPARQL for domain-specific purposes.
For example, users can register custom SPARQL functions commonly found in other SPARQL engines.
In the future, we plan to provide additional extension points.
See [examples](../../examples) for examples that make use of the `Store` API.

## Library Use

RDF Fusion can also be used as a "library" for DataFusion.
In this scenario, users directly interact with DataFusion's query engine and make use of RDF Fusion's operators and
rewriting rules that we use to implement SPARQL.
This allows users to combine operators from DataFusion, RDF Fusion, and even other systems building on DataFusion within
a single query.
Users that want to *build something new* using RDF Fusion's capabilities are advised to use this API.
See [examples](../../examples) for further details.

# A Brief Introduction of RDF and SPARQL

If you're familiar with relational databases you might be wondering how we can implement SPARQL queries on a relational
query engine.
These two query languages seem so different at first glance.
And while these query languages are based on different features (e.g., graph pattern matching vs tables), under the
hood, SPARQL can be mapped onto a relational query engine.
RDF Fusion adopts this model and to allow users to use RDF Fusion without digging through the SPARQL standard, here is
a brief introduction of RDF and SPARQL.

## The Resource Description Framework

The [Resource Description Framework](https://www.w3.org/TR/rdf11-concepts/) is the data model that underpins SPARQL.
Data in RDF are represented as triples, where each triple consists of a subject, a predicate, and an object.
The subject and predicate are typically [IRIs](https://en.wikipedia.org/wiki/Internationalized_Resource_Identifier),
while the object can be an IRI or a literal.
Think of IRIs as a global identifier, while literals are values that can be used to describe a resource.
An RDF graph is simply a set of triples.
Here is an annotated example from the [Turtle Specification](https://www.w3.org/TR/turtle/), a format for serializing
RDF triples.

```turtle
# Base Address to resolve relative IRIs
BASE <http://example.org/>

# Some prefixes to make it easier to spell out other IRIs we are using
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
PREFIX foaf: <http://xmlns.com/foaf/0.1/> .
PREFIX rel: <http://www.perceive.net/schemas/relationship/> .

<#spiderman>
    rel:enemyOf <#green-goblin> ;               # The Green Goblin is an enemy of Spiderman.
    a foaf:Person ;                             # Spiderman is a Person.
    foaf:name "Spiderman", "Человек-паук"@ru .  # You can even add language tags to your literals
```

Note that the same data from above can be depicted as a graph.
All subjects and objects become nodes in the graph, while the predicates become edges.

## Graph Patterns

## SPARQL

TODO Solutions, Projections, Relational

# Architecture

Before diving into the details of RDF Fusion, it makes sense to have a high-level
overview [DataFusion's architecture](https://docs.rs/datafusion/latest/datafusion/index.html#architecture).
As RDF Fusion is built on top of DataFusion, it shares the same architecture of the query engine.
Nevertheless, there are interesting aspects of how we extend DataFusion to support SPARQL, thus warranting its own
section.
Here, we will briefly discuss various aspects of RDF Fusion and then link to the more detailed documentation.

## Encoding RDF Terms in Arrow

Encoding RDF terms in Arrow is the core aspect of RDF Fusion.
Solutions in SPARQL (i.e., "rows" in the result[^2]) have a few peculiarities that may be surprising to some users
coming
from relational databases.
The most important one being that

## Crates

TO

[^1]: I don't
[^2]: https://www.w3.org/TR/sparql11-results-json/#results