use crate::error::StorageError;
use crate::results::QuerySolutionStream;
use crate::sparql::error::EvaluationError;
use arrow_rdf::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use futures::{ready, Stream, StreamExt};
use oxrdf::{GraphName, NamedNode, Quad, Subject, Term};
use sparesults::QuerySolution;
use std::pin::Pin;
use std::task::{Context, Poll};

/// An iterator returning quads
pub struct QuadStream {
    inner: QuerySolutionStream,
}

impl QuadStream {
    pub async fn try_collect(mut self) -> Result<Vec<Quad>, EvaluationError> {
        let mut result = Vec::new();
        while let Some(element) = self.next().await {
            result.push(element?);
        }
        Ok(result)
    }

    pub fn try_new(inner: QuerySolutionStream) -> Result<Self, String> {
        let variables = inner
            .variables()
            .iter()
            .map(|v| v.as_str())
            .collect::<Vec<&str>>();
        if !matches!(
            variables.as_slice(),
            &[COL_GRAPH, COL_SUBJECT, COL_PREDICATE, COL_OBJECT]
        ) {
            return Err(String::from("Unexpected schema of solution stream"));
        }
        Ok(Self { inner })
    }
}

impl Stream for QuadStream {
    type Item = Result<Quad, EvaluationError>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inner_poll = ready!(self.inner.poll_next_unpin(ctx));
        match inner_poll {
            None => Poll::Ready(None),
            Some(inner_result) => Poll::Ready(Some(inner_result.and_then(to_quad))),
        }
    }
}

fn to_quad(solution: QuerySolution) -> Result<Quad, EvaluationError> {
    // TODO: error handling
    let graph_name = to_graph_name(solution.get(COL_GRAPH).expect("Grpah not found").clone())?;
    let subject = to_subject(
        solution
            .get(COL_SUBJECT)
            .expect("Subject not found")
            .clone(),
    )?;
    let predicate = to_predicate(
        solution
            .get(COL_PREDICATE)
            .expect("Predicate not found")
            .clone(),
    )?;
    let object = solution.get(COL_OBJECT).expect("Object not found").clone();
    Ok(Quad::new(subject, predicate, object, graph_name))
}

fn to_graph_name(term: Term) -> Result<GraphName, EvaluationError> {
    Ok(match term {
        Term::NamedNode(n) => GraphName::from(n),
        Term::BlankNode(n) => GraphName::from(n),
        Term::Literal(literal) if literal.value() == "DEFAULT" => GraphName::DefaultGraph,
        _ => {
            unimplemented!("Proper error handling")
        }
    })
}

fn to_subject(term: Term) -> Result<Subject, EvaluationError> {
    Ok(match term {
        Term::NamedNode(n) => Subject::from(n),
        Term::BlankNode(n) => Subject::from(n),
        _ => {
            unimplemented!("Proper error handling")
        }
    })
}

fn to_predicate(term: Term) -> Result<NamedNode, StorageError> {
    Ok(match term {
        Term::NamedNode(n) => NamedNode::from(n),
        _ => {
            unimplemented!("Proper error handling")
        }
    })
}
