use axum::body::{Body, Bytes};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures::{Stream, StreamExt};
use rdf_fusion::io::{RdfFormat, RdfSerializer};
use rdf_fusion::results::{QueryResultsFormat, QueryResultsSerializer};
use rdf_fusion::{QueryResults, QuerySolutionStream, QueryTripleStream};
use serde::de::StdError;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

/// Wraps a [QueryResults] that can be converted into a [Response].
#[allow(unused)]
pub struct QueryResultsResponse(QueryResults, RdfFormat, QueryResultsFormat);

impl QueryResultsResponse {
    /// Creates a new [QueryResultsResponse].
    pub fn new(
        query_results: QueryResults,
        rdf_format: RdfFormat,
        query_result_format: QueryResultsFormat,
    ) -> Self {
        Self(query_results, rdf_format, query_result_format)
    }
}

impl IntoResponse for QueryResultsResponse {
    fn into_response(self) -> Response {
        match self.0 {
            QueryResults::Solutions(solutions) => {
                let solutions = QuerySolutionStreamResponseBody(solutions, self.2);
                let body = Body::from_stream(solutions);
                Response::builder()
                    .status(StatusCode::OK)
                    .body(body)
                    .unwrap()
            }
            QueryResults::Boolean(value) => {
                let mut buffer = Vec::new();
                let serializer = QueryResultsSerializer::from_format(self.2);
                serializer
                    .serialize_boolean_to_writer(&mut buffer, value)
                    .unwrap();

                Response::builder()
                    .header("Content-Type", "application/sparql-results+json")
                    .status(StatusCode::OK)
                    .body(Body::from(buffer))
                    .unwrap()
            }
            QueryResults::Graph(triples) => {
                let solutions = QueryTripleStreamResponseBody(triples, self.1);
                let body = Body::from_stream(solutions);
                Response::builder()
                    .status(StatusCode::OK)
                    .body(body)
                    .unwrap()
            }
        }
    }
}

struct QuerySolutionStreamResponseBody(QuerySolutionStream, QueryResultsFormat);

impl Stream for QuerySolutionStreamResponseBody {
    type Item = Result<Bytes, Box<dyn StdError + Send + Sync + 'static>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut buffer = Vec::new();
        let mut serializer = QueryResultsSerializer::from_format(self.1)
            .serialize_solutions_to_writer(&mut buffer, self.0.variables().to_vec())
            .unwrap();

        let mut count = 0;
        while let Some(solution) = ready!(self.0.poll_next_unpin(cx)) {
            serializer.serialize(solution.unwrap().into_iter()).unwrap();
            count += 1;

            if count == 1024 {
                break;
            }
        }

        if count == 0 {
            return Poll::Ready(None);
        }

        Poll::Ready(Some(Ok(Bytes::from(buffer))))
    }
}

struct QueryTripleStreamResponseBody(QueryTripleStream, RdfFormat);

impl Stream for QueryTripleStreamResponseBody {
    type Item = Result<Bytes, Box<dyn StdError + Send + Sync + 'static>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut buffer = Vec::new();
        let serializer = RdfSerializer::from_format(self.1);
        let mut serializer = serializer.for_writer(&mut buffer);

        while let Some(triple) = ready!(self.0.poll_next_unpin(cx)) {
            serializer.serialize_triple(triple.unwrap().as_ref()).unwrap();
        }

        serializer.finish().unwrap();

        Poll::Ready(Some(Ok(Bytes::from(buffer))))
    }
}
