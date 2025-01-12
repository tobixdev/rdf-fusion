use crate::error::StorageError;
use crate::store::Store;
use arrow_rdf::decoded::model::{
    DEC_TYPE_ID_BLANK_NODE, DEC_TYPE_ID_NAMED_NODE, DEC_TYPE_ID_STRING, DEC_TYPE_ID_TYPED_LITERAL,
};
use arrow_rdf::decoded::DEC_QUAD_SCHEMA;
use datafusion::arrow::array::{AsArray, RecordBatch, UnionArray};
use datafusion::execution::SendableRecordBatchStream;
use futures::{Stream, StreamExt};
use oxrdf::{BlankNode, GraphName, NamedNode, Quad, Subject, Term};
use std::pin::Pin;
use std::task::{ready, Context, Poll};

/// An iterator returning the quads contained in a [`Store`].
pub struct QuadStream {
    inner: Option<SendableRecordBatchStream>,
    current: Option<<Vec<Quad> as IntoIterator>::IntoIter>,
}

impl QuadStream {
    pub async fn try_read_all(mut self) -> Result<Vec<Quad>, StorageError> {
        let mut result = Vec::new();
        while let Some(element) = self.next().await {
            result.push(element?);
        }
        Ok(result)
    }

    fn poll_inner(&mut self, ctx: &mut Context<'_>) -> Poll<Option<Result<Quad, StorageError>>> {
        match (&mut self.inner, &mut self.current) {
            // Still entries from the current batch to return
            (_, Some(iter)) => {
                let next = iter.next();
                match next {
                    None => {
                        self.current = None;
                        self.poll_inner(ctx)
                    }
                    Some(quad) => Poll::Ready(Some(Ok(quad))),
                }
            }
            // Load new batch
            (Some(stream), None) => {
                let next_batch = ready!(stream.poll_next_unpin(ctx));
                match next_batch {
                    None => {
                        self.inner = None;
                        self.poll_inner(ctx)
                    }
                    Some(batch) => {
                        // TODO: error handling
                        self.current = Some(to_quads(&batch.unwrap()).unwrap());
                        self.poll_inner(ctx)
                    }
                }
            }
            // Empty
            (None, None) => Poll::Ready(None),
        }
    }
}

impl Stream for QuadStream {
    type Item = Result<Quad, StorageError>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(ctx)
    }
}

fn to_quads(batch: &RecordBatch) -> Result<<Vec<Quad> as IntoIterator>::IntoIter, StorageError> {
    // TODO: error handling

    // Schema is validated
    let graphs = batch.column_by_name("graph").unwrap().as_union();
    let subjects = batch.column_by_name("subject").unwrap().as_union();
    let predicates = batch.column_by_name("graph").unwrap().as_union();
    let objects = batch.column_by_name("object").unwrap().as_union();

    let mut result = Vec::new();
    for i in 0..batch.num_rows() {
        let type_id = graphs.type_id(i);
        let graph_name = to_graph_name(graphs, i, type_id)?;
        let subject = to_subject(subjects, i, type_id)?;
        let predicate = to_predicate(predicates, i, type_id)?;
        let object = to_object(objects, i, type_id)?;
        result.push(Quad::new(subject, predicate, object, graph_name))
    }
    Ok(result.into_iter())
}

fn to_graph_name(graphs: &UnionArray, i: usize, type_id: i8) -> Result<GraphName, StorageError> {
    Ok(match type_id {
        DEC_TYPE_ID_NAMED_NODE => {
            let value = graphs.child(type_id).as_string::<i32>().value(i);
            GraphName::NamedNode(NamedNode::new(value).unwrap())
        }
        _ => {
            unimplemented!("Proper error handling")
        }
    })
}

fn to_subject(subjects: &UnionArray, i: usize, type_id: i8) -> Result<Subject, StorageError> {
    Ok(match type_id {
        DEC_TYPE_ID_NAMED_NODE => {
            let value = subjects.child(type_id).as_string::<i32>().value(i);
            Subject::NamedNode(NamedNode::new(value).unwrap())
        }
        DEC_TYPE_ID_BLANK_NODE => {
            let value = subjects.child(type_id).as_string::<i32>().value(i);
            Subject::NamedNode(NamedNode::new(value).unwrap())
        }
        _ => {
            unimplemented!("Proper error handling")
        }
    })
}

fn to_predicate(predicates: &UnionArray, i: usize, type_id: i8) -> Result<NamedNode, StorageError> {
    Ok(match type_id {
        DEC_TYPE_ID_NAMED_NODE => {
            let value = predicates.child(type_id).as_string::<i32>().value(i);
            NamedNode::new(value).unwrap()
        }
        _ => {
            unimplemented!("Proper error handling")
        }
    })
}

fn to_object(objects: &UnionArray, i: usize, type_id: i8) -> Result<Term, StorageError> {
    Ok(match type_id {
        DEC_TYPE_ID_NAMED_NODE => {
            let value = objects.child(type_id).as_string::<i32>().value(i);
            Term::NamedNode(NamedNode::new(value).unwrap())
        }
        DEC_TYPE_ID_BLANK_NODE => {
            let value = objects.child(type_id).as_string::<i32>().value(i);
            Term::BlankNode(BlankNode::new(value).unwrap())
        }
        DEC_TYPE_ID_STRING => {
            unimplemented!("Nested Type")
        }
        DEC_TYPE_ID_TYPED_LITERAL => {
            unimplemented!("Nested Type")
        }
        _ => {
            unimplemented!("Proper error handling")
        }
    })
}

impl QuadStream {
    pub fn try_new(stream: SendableRecordBatchStream) -> Result<Self, String> {
        if !stream.schema().eq(&DEC_QUAD_SCHEMA) {
            return Err(String::from("Unexpected schema of record stream"));
        }

        Ok(Self {
            inner: Some(stream),
            current: None,
        })
    }
}
