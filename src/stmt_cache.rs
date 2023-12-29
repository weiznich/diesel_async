use std::collections::HashMap;
use std::hash::Hash;

use diesel::backend::Backend;
use diesel::connection::statement_cache::{MaybeCached, PrepareForCache, StatementCacheKey};
use diesel::connection::Instrumentation;
use diesel::connection::InstrumentationEvent;
use diesel::QueryResult;
use futures_util::{future, FutureExt};

#[derive(Default)]
pub struct StmtCache<DB: Backend, S> {
    cache: HashMap<StatementCacheKey<DB>, S>,
}

type PrepareFuture<'a, F, S> = future::Either<
    future::Ready<QueryResult<(MaybeCached<'a, S>, F)>>,
    future::BoxFuture<'a, QueryResult<(MaybeCached<'a, S>, F)>>,
>;

#[async_trait::async_trait]
pub trait PrepareCallback<S, M>: Sized {
    async fn prepare(
        self,
        sql: &str,
        metadata: &[M],
        is_for_cache: PrepareForCache,
    ) -> QueryResult<(S, Self)>;
}

impl<S, DB: Backend> StmtCache<DB, S> {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub fn cached_prepared_statement<'a, F>(
        &'a mut self,
        cache_key: StatementCacheKey<DB>,
        sql: String,
        is_query_safe_to_cache: bool,
        metadata: &[DB::TypeMetadata],
        prepare_fn: F,
        instrumentation: &std::sync::Mutex<Option<Box<dyn Instrumentation>>>,
    ) -> PrepareFuture<'a, F, S>
    where
        S: Send,
        DB::QueryBuilder: Default,
        DB::TypeMetadata: Clone + Send + Sync,
        F: PrepareCallback<S, DB::TypeMetadata> + Send + 'a,
        StatementCacheKey<DB>: Hash + Eq,
    {
        use std::collections::hash_map::Entry::{Occupied, Vacant};

        if !is_query_safe_to_cache {
            let metadata = metadata.to_vec();
            let f = async move {
                let stmt = prepare_fn
                    .prepare(&sql, &metadata, PrepareForCache::No)
                    .await?;
                Ok((MaybeCached::CannotCache(stmt.0), stmt.1))
            }
            .boxed();
            return future::Either::Right(f);
        }

        match self.cache.entry(cache_key) {
            Occupied(entry) => future::Either::Left(future::ready(Ok((
                MaybeCached::Cached(entry.into_mut()),
                prepare_fn,
            )))),
            Vacant(entry) => {
                let metadata = metadata.to_vec();
                instrumentation
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .on_connection_event(InstrumentationEvent::cache_query(&sql));
                let f = async move {
                    let statement = prepare_fn
                        .prepare(&sql, &metadata, PrepareForCache::Yes)
                        .await?;

                    Ok((MaybeCached::Cached(entry.insert(statement.0)), statement.1))
                }
                .boxed();
                future::Either::Right(f)
            }
        }
    }
}
