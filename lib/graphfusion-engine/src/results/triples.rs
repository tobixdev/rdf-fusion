use crate::results::QuerySolutionStream;
use crate::sparql::error::EvaluationError;
use futures::{Stream, StreamExt};
use oxrdf::{BlankNode, Graph, Term, Triple, Variable};
use sparesults::QuerySolution;
use spargebra::term::{TermPattern, TriplePattern};
use std::collections::HashSet;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

/// An iterator over the triples that compose a graph solution.
///
/// ```
/// use graphfusion::sparql::QueryResults;
/// use graphfusion::store::Store;
///
/// let store = Store::new()?;
/// if let QueryResults::Graph(triples) = store.query("CONSTRUCT WHERE { ?s ?p ?o }")? {
///     for triple in triples {
///         println!("{}", triple?);
///     }
/// }
/// # Result::<_, Box<dyn std::error::Error>>::Ok(())
/// ```
pub struct QueryTripleStream {
    template: Vec<TriplePattern>,
    inner: QuerySolutionStream,

    buffered_results: Vec<Result<Triple, EvaluationError>>,
    already_emitted_results: HashSet<Triple>,
    bnodes: Vec<BlankNode>,
}

impl QueryTripleStream {
    pub fn new(template: Vec<TriplePattern>, inner: QuerySolutionStream) -> Self {
        Self {
            template,
            inner,
            buffered_results: vec![],
            already_emitted_results: HashSet::new(),
            bnodes: vec![],
        }
    }

    pub async fn collect_as_graph(&mut self) -> Result<Graph, EvaluationError> {
        let mut graph = Graph::new();
        while let Some(triple) = self.next().await {
            let triple = triple?;
            graph.insert(triple.as_ref());
        }
        Ok(graph)
    }

    fn poll_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Triple, EvaluationError>>> {
        // If we have buffered results, use them
        if let Some(result) = self.buffered_results.pop() {
            return Poll::Ready(Some(result));
        }

        // If we do not have buffered results, create them
        let solution = match ready!(self.inner.poll_next_unpin(cx)) {
            None => return Poll::Ready(None),
            Some(Ok(tuple)) => tuple,
            Some(Err(error)) => return Poll::Ready(Some(Err(error))),
        };

        for template in &self.template {
            let subject = get_triple_template_value(&template.subject, &solution, &mut self.bnodes)
                .and_then(|t| t.try_into().ok());
            let predicate = get_triple_template_value(
                &TermPattern::from(template.predicate.clone()),
                &solution,
                &mut self.bnodes,
            )
            .and_then(|t| t.try_into().ok());
            let object = get_triple_template_value(&template.object, &solution, &mut self.bnodes);

            if let (Some(subject), Some(predicate), Some(object)) = (subject, predicate, object) {
                let triple = Triple {
                    subject,
                    predicate,
                    object,
                };
                // We allocate new blank nodes for each solution,
                // triples with blank nodes are likely to be new.
                let new_triple = triple.subject.is_blank_node()
                    || triple.subject.is_triple()
                    || triple.object.is_blank_node()
                    || triple.object.is_triple()
                    || self.already_emitted_results.insert(triple.clone());
                if new_triple {
                    self.buffered_results.push(Ok(triple));
                }
            }
        }
        self.bnodes.clear(); // We do not reuse blank nodes

        // re-run procedure
        self.poll_inner(cx)
    }
}

fn get_triple_template_value(
    selector: &TermPattern,
    tuple: &QuerySolution,
    bnodes: &mut Vec<BlankNode>,
) -> Option<Term> {
    match selector {
        TermPattern::NamedNode(nn) => Some(Term::NamedNode(nn.clone())),
        TermPattern::BlankNode(bnode) => {
            bnodes.push(bnode.clone());
            Some(Term::BlankNode(bnode.clone()))
        }
        TermPattern::Literal(term) => Some(Term::Literal(term.clone())),
        TermPattern::Variable(v) => tuple.get(v).cloned(),
        TermPattern::Triple(triple) => Some(
            Triple {
                subject: get_triple_template_value(&triple.subject, tuple, bnodes)?
                    .try_into()
                    .ok()?,
                predicate: get_triple_template_value(
                    &TermPattern::from(triple.predicate.clone()),
                    tuple,
                    bnodes,
                )?
                .try_into()
                .ok()?,
                object: get_triple_template_value(&triple.object, tuple, bnodes)?,
            }
            .into(),
        ),
    }
}

impl Stream for QueryTripleStream {
    type Item = Result<Triple, EvaluationError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (min, max) = self.inner.size_hint();
        (
            min.saturating_mul(self.template.len()),
            max.map(|v| v.saturating_mul(self.template.len())),
        )
    }
}
