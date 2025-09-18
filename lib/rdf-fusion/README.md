RDF Fusion
========


RDF Fusion is an experimental columnar [SPARQL](https://www.w3.org/TR/sparql11-overview/) engine.  
It is built on [Apache DataFusion](https://datafusion.apache.org/), an extensible query engine that
uses [Apache Arrow](https://arrow.apache.org/) as its in-memory data format.

# Using RDF Fusion

RDF Fusion can currently be used in two ways: via the convenient `Store` API or as a library for DataFusion.

## Store API

The `Store` API provides high-level methods for interacting with the database, such as inserting data and running
queries.  
Users who primarily want to *use* RDF Fusion are encouraged to use this API.

While the `Store` API is based on [Oxigraph](https://github.com/oxigraph/oxigraph)'s `Store`, full compatibility is
not a goal.
Some aspects of RDF Fusion differ fundamentally, for example, its use of `async` methods.

The `Store` API also supports extending SPARQL for domain-specific purposes.  
For instance, users can register custom SPARQL functions, similar to those found in other SPARQL engines.
Additional extension points are planned for future releases.

See the [examples](../../examples) directory for demonstrations of the `Store` API in action.

## Library Use

RDF Fusion can also be used as a library for DataFusion.  
In this mode, users interact directly with DataFusion's query engine and leverage RDF Fusion's operators and rewriting
rules used to implement SPARQL.

This approach allows combining operators from DataFusion, RDF Fusion, and even other systems built on DataFusion within
a single query.  
Users who want to *build new systems* using RDF Fusion's SPARQL implementation are encouraged to use this API.

See the [examples](../../examples) directory for more details.

# A Brief Introduction to RDF and SPARQL

If you're familiar with relational databases, you might wonder how SPARQL queries can be implemented on a relational
query engine.
At first glance, these query languages appear quite different.

However, despite surface-level differences, SPARQL engines share many similarities with relational query engines.
This common ground allows RDF Fusion to provide SPARQL support on top of DataFusion without re-implementing large
portions of the query engine.

For readers familiar with relational databases who want to start using RDF Fusion without diving deep into the SPARQL
standard, this section provides a brief introduction to RDF and SPARQL.
If you are already familiar with these technologies, you can safely skip this section.

Some details are simplified to make the introduction more accessible.
For example, we will completely ignore [blank nodes](https://www.w3.org/TR/rdf11-concepts/#dfn-blank-node).
For a detailed specification, please refer to the official RDF and SPARQL standards.

## The Resource Description Framework

The [Resource Description Framework (RDF)](https://www.w3.org/TR/rdf11-concepts/) is the data model that underpins
SPARQL.  
Data in RDF are represented as **triples**, where each triple consists of a **subject**, a **predicate**, and an
**object**.

- The **subject** and **predicate** are
  typically [IRIs](https://en.wikipedia.org/wiki/Internationalized_Resource_Identifier).
- The **object** can be either an IRI or a **literal**.

Think of IRIs as global identifiers that look similar to web links, while literals are standard values like strings,
numbers, or dates.
Lastly, an **RDF term** is either an IRI or a literal.

For example, the following triple states that Spiderman (an IRI) has the name "Spiderman" (a literal):

```
(<http://example.org/spiderman>, <http://xmlns.com/foaf/0.1/name>, "Spiderman")
```

An **RDF graph** is simply a set of triples.  
The following example, taken from the [Turtle Specification](https://www.w3.org/TR/turtle/), shows a small graph
containing information about Spiderman, the Green Goblin, and their relationship.

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

At first, it may not be obvious how a set of triples represents a graph.  
In an RDF graph, subjects and objects correspond to **nodes**, while predicates label the **edges** connecting them.
It is important that the same IRI always corresponds to the same node, even across multiple triples.

## Graph Patterns

Given an RDF graph, we can ask questions about the data.  
For example: *"Who are the enemies of Spiderman?"*

These questions can be expressed as **graph patterns**, a core concept in the SPARQL standard.  
A graph pattern is essentially a triple in which one or more components may be **variables**.

For example, the following graph pattern expresses the question above (assuming the prefixes defined previously):

```text
<#spiderman> rel:enemyOf ?enemy
```

Evaluating graph patterns against an RDF graph is often referred to as **graph pattern matching**.
In this process, we look for triples in the graph that match the components of the graph pattern.
For example, the graph pattern above matches the following triple from the RDF graph introduced earlier:

```text
<#spiderman> rel:enemyOf <#green-goblin>
```

However, the result of graph pattern matching is not the triples themselves, but a **solution**.  
A solution is a set of bindings for the variables in the graph pattern.
Here, we will depict solutions as a table, reflecting how SPARQL query execution can be mapped onto a relational query
engine (more on this later).
The result of the above graph pattern matching is the following table:

| **?enemy**      |
|-----------------|
| <#green-goblin> |

## SPARQL

Some relevant fundamental concepts of SPARQL were already covered in the previous section.  
Here, we will dive a bit deeper to try to understand the connection between SPARQL and relational query engines.

First, let's look at a simple SPARQL query.  
The query below searches for all persons whose enemies contain the Green Goblin.  
Note that the variable `?superhero` is used multiple times in the query.

```text
SELECT ?superhero
{
    ?superhero a foaf:Person .
    ?superhero rel:enemyOf <#green-goblin> .
}
```

The SPARQL standard specifies that the query above must find all solutions (i.e., bindings of `?superhero`) that
satisfy **both** graph patterns.

In our approach, multiple graph patterns can be combined by **joining** them on the variables they share.  
In the example above, the two graph patterns can be joined on the variable `?superhero`.
This produces the following (simplified) query plan:

```text
Inner Join: lhs.superhero = rhs.superhero
  SubqueryAlias: lhs
    TriplePattern: ?superhero a foaf:Person 
  SubqueryAlias: rhs
    TriplePattern ?superhero rel:enemyOf <#green-goblin>
```

Users familiar with DataFusion should feel right at home.  
We have effectively transformed the core operator of SPARQL into relational query operators!
Fortunately, DataFusion allows us to extend its set of operators with custom ones, such as `TriplePattern`.

Next, let's consider an important challenge within this approach:  
What is the result of the following query that retrieves all information about Spiderman?

```text
SELECT ?object
{
    <#spiderman> ?predicate ?object
}
```

Here is the result of the query:

| **?object**       |
|-------------------|
| <#green-goblin>   |
| foaf:Person       |
| "Spiderman"       |
| "Человек-паук"@ru |

Those familiar with relational databases might naturally wonder about the data type of this column.  
After all, a single column can contain IRIs, plain strings, and language-tagged strings.
It may also include other literal types such as booleans, numbers, and dates.

Such variability is not typically possible in standard relational models.  
While the mathematical domain of the column is simple (the set of RDF terms), representing it efficiently in a
relational query engine is not trivial.

This challenge motivates **RDF Fusion's RDF term encodings**, which bridge the gap between the dynamic nature of SPARQL
solutions and the expressive type system of Apache Arrow.
We will cover this topic in more detail in the next section.

# Architecture

Before diving into the details of RDF Fusion, it makes sense to have a high-level
overview [DataFusion's architecture](https://docs.rs/datafusion/latest/datafusion/index.html#architecture).
As RDF Fusion is built on top of DataFusion, it shares the same architecture of the query engine.
Nevertheless, there are interesting aspects of how we extend DataFusion to support SPARQL, thus warranting its own
section.
Here, we will briefly discuss various aspects of RDF Fusion and then link to the more detailed documentation.

## Encoding RDF Terms in Arrow

Encoding RDF terms in Arrow is the core aspect of RDF Fusion.
Solutions in SPARQL (i.e., "rows" in the result) have a few peculiarities that may be surprising to some users
coming
from relational databases.
The most important one being that

## Crates

TO
