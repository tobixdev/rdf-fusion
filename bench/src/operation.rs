use crate::runs::BenchmarkRun;
use futures::StreamExt;
use rdf_fusion::store::Store;
use rdf_fusion::{Query, QueryExplanation, QueryOptions, QueryResults};

#[derive(Clone)]
pub enum SparqlRawOperation<QueryName> {
    Query(QueryName, String),
}

impl<QueryName: Clone> SparqlRawOperation<QueryName> {
    pub fn parse(&self) -> anyhow::Result<SparqlOperation<QueryName>> {
        match self {
            SparqlRawOperation::Query(query_name, query) => {
                let query = query.parse()?;
                Ok(SparqlOperation::Query(query_name.clone(), query))
            }
        }
    }
}

#[derive(Clone)]
pub enum SparqlOperation<QueryName> {
    Query(QueryName, Query),
}

impl<QueryName> SparqlOperation<QueryName> {
    pub fn query(&self) -> &Query {
        match self {
            SparqlOperation::Query(_, query) => query,
        }
    }

    pub async fn run(
        &self,
        store: &Store,
    ) -> anyhow::Result<(BenchmarkRun, QueryExplanation)> {
        let start = datafusion::common::instant::Instant::now();

        let options = QueryOptions::default();
        let explanation = match &self {
            SparqlOperation::Query(_, q) => {
                let (result, explanation) =
                    store.explain_query_opt(q.clone(), options.clone()).await?;
                match result {
                    QueryResults::Boolean(_) => (),
                    QueryResults::Solutions(s) => {
                        let mut stream = s.into_record_batch_stream()?;
                        while let Some(s) = stream.next().await {
                            s?;
                        }
                    }
                    QueryResults::Graph(mut g) => {
                        while let Some(t) = g.next().await {
                            t?;
                        }
                    }
                }
                explanation
            }
        };

        let duration = start.elapsed();
        Ok((BenchmarkRun { duration }, explanation))
    }
}

impl<QueryName: Clone> SparqlOperation<QueryName> {
    pub fn query_name(&self) -> QueryName {
        match self {
            SparqlOperation::Query(name, _) => name.clone(),
        }
    }
}
