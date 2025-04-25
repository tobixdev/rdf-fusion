use crate::results::QuerySolutionStream;
use crate::sparql::error::EvaluationError;
use arrow_rdf::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use futures::{ready, Stream, StreamExt};
use model::{DecodedTerm, GraphName, NamedNode, Quad, Subject, Variable};
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
            .map(Variable::as_str)
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

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let inner_poll = ready!(self.inner.poll_next_unpin(cx));
        match inner_poll {
            None => Poll::Ready(None),
            Some(inner_result) => {
                Poll::Ready(Some(inner_result.and_then(|solution| to_quad(&solution))))
            }
        }
    }
}

#[allow(
    clippy::expect_used,
    reason = "Schema already checked in QuadStream::try_new"
)]
fn to_quad(solution: &QuerySolution) -> Result<Quad, EvaluationError> {
    let graph_name = to_graph_name(solution.get(COL_GRAPH))?;
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

fn to_graph_name(term: Option<&DecodedTerm>) -> Result<GraphName, EvaluationError> {
    match term {
        None => Ok(GraphName::DefaultGraph),
        Some(DecodedTerm::NamedNode(n)) => Ok(GraphName::from(n.clone())),
        Some(DecodedTerm::BlankNode(n)) => Ok(GraphName::from(n.clone())),
        _ => EvaluationError::internal("Predicate has invalid value in quads.".into()),
    }
}

fn to_subject(term: DecodedTerm) -> Result<Subject, EvaluationError> {
    match term {
        DecodedTerm::NamedNode(n) => Ok(Subject::from(n)),
        DecodedTerm::BlankNode(n) => Ok(Subject::from(n)),
        _ => EvaluationError::internal("Predicate has invalid value in quads.".into()),
    }
}

fn to_predicate(term: DecodedTerm) -> Result<NamedNode, EvaluationError> {
    match term {
        DecodedTerm::NamedNode(n) => Ok(n),
        _ => EvaluationError::internal("Predicate has invalid value in quads.".to_owned()),
    }
}
