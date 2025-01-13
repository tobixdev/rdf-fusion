use crate::error::StorageError;
use crate::store::Store;
use arrow_rdf::decoded::model::{
    DEC_TYPE_ID_BLANK_NODE, DEC_TYPE_ID_NAMED_NODE, DEC_TYPE_ID_STRING, DEC_TYPE_ID_TYPED_LITERAL,
};
use arrow_rdf::decoded::DEC_QUAD_SCHEMA;
use arrow_rdf::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use datafusion::arrow::array::{Array, AsArray, RecordBatch, UnionArray};
use datafusion::common::SchemaExt;
use datafusion::execution::SendableRecordBatchStream;
use futures::{Stream, StreamExt};
use oxrdf::{BlankNode, GraphName, Literal, NamedNode, Quad, Subject, Term};
use std::any::Any;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

/// An iterator returning the quads contained in a [`Store`].
pub struct QuadStream {
    inner: Option<SendableRecordBatchStream>,
    current: Option<<Vec<Quad> as IntoIterator>::IntoIter>,
}

impl QuadStream {
    pub async fn try_collect(mut self) -> Result<Vec<Quad>, StorageError> {
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
    let graphs = batch.column_by_name(COL_GRAPH).unwrap().as_union();
    let subjects = batch.column_by_name(COL_SUBJECT).unwrap().as_union();
    let predicates = batch.column_by_name(COL_PREDICATE).unwrap().as_union();
    let objects = batch.column_by_name(COL_OBJECT).unwrap().as_union();

    let mut result = Vec::new();
    for i in 0..batch.num_rows() {
        let graph_name = to_graph_name(graphs, graphs.value_offset(i), graphs.type_id(i))?;
        let subject = to_subject(subjects, subjects.value_offset(i), subjects.type_id(i))?;
        let predicate = to_predicate(
            predicates,
            predicates.value_offset(i),
            predicates.type_id(i),
        )?;
        let object = to_object(objects, objects.value_offset(i), objects.type_id(i))?;
        result.push(Quad::new(subject, predicate, object, graph_name))
    }

    Ok(result.into_iter())
}

fn to_graph_name(graphs: &UnionArray, i: usize, type_id: i8) -> Result<GraphName, StorageError> {
    Ok(match type_id {
        DEC_TYPE_ID_NAMED_NODE => {
            let value = graphs.child(type_id).as_string::<i32>().value(i);
            if value == "DEFAULT" {
                GraphName::DefaultGraph
            } else {
                GraphName::NamedNode(NamedNode::new(value).unwrap())
            }
        }
        DEC_TYPE_ID_BLANK_NODE => {
            let value = graphs.child(type_id).as_string::<i32>().value(i);
            GraphName::BlankNode(BlankNode::new(value).unwrap())
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
            Subject::BlankNode(BlankNode::new(value).unwrap())
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
            unimplemented!("Proper error handling: {}", type_id)
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
            let values = objects
                .child(type_id)
                .as_struct()
                .column_by_name("value")
                .unwrap()
                .as_string::<i32>();
            let language = objects
                .child(type_id)
                .as_struct()
                .column_by_name("language")
                .unwrap()
                .as_string::<i32>();

            if language.is_null(i) {
                // TODO: error handling?
                Term::Literal(Literal::new_simple_literal(String::from(values.value(i))))
            } else {
                // TODO: error handling?
                Term::Literal(
                    Literal::new_language_tagged_literal(
                        String::from(values.value(i)),
                        String::from(language.value(i)),
                    )
                    .unwrap(),
                )
            }
        }
        DEC_TYPE_ID_TYPED_LITERAL => {
            let values = objects
                .child(type_id)
                .as_struct()
                .column_by_name("value")
                .unwrap()
                .as_string::<i32>();
            let datatypes = objects
                .child(type_id)
                .as_struct()
                .column_by_name("datatype")
                .unwrap()
                .as_string::<i32>();
            Term::Literal(Literal::new_typed_literal(
                String::from(values.value(i)),
                NamedNode::new(String::from(datatypes.value(i))).unwrap(), // TODO: error handling?
            ))
        }
        _ => {
            unimplemented!("Proper error handling")
        }
    })
}

impl QuadStream {
    pub fn try_new(stream: SendableRecordBatchStream) -> Result<Self, String> {
        if !stream.schema().equivalent_names_and_types(&DEC_QUAD_SCHEMA) {
            return Err(String::from("Unexpected schema of record stream"));
        }

        Ok(Self {
            inner: Some(stream),
            current: None,
        })
    }
}
