use std::collections::HashMap;
use std::hash::Hash;

use diesel::backend::Backend;
use diesel::connection::statement_cache::{MaybeCached, PrepareForCache, StatementCacheKey};
use diesel::query_builder::{QueryFragment, QueryId};
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
pub trait PrepareCallback<S, M> {
    async fn prepare(
        self,
        sql: &str,
        metadata: &[M],
        is_for_cache: PrepareForCache,
    ) -> QueryResult<(S, Self)>
    where
        Self: Sized;
}

impl<S, DB: Backend> StmtCache<DB, S> {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub fn cached_prepared_statement<'a, T, F>(
        &'a mut self,
        query: T,
        metadata: &[DB::TypeMetadata],
        prepare_fn: F,
        backend: &DB,
    ) -> PrepareFuture<'a, F, S>
    where
        S: Send,
        DB::QueryBuilder: Default,
        DB::TypeMetadata: Clone + Send + Sync,
        T: QueryFragment<DB> + QueryId + Send,
        F: PrepareCallback<S, DB::TypeMetadata> + Send + 'a,
        StatementCacheKey<DB>: Hash + Eq,
    {
        use std::collections::hash_map::Entry::{Occupied, Vacant};

        let cache_key = match StatementCacheKey::for_source(&query, metadata, backend) {
            Ok(key) => key,
            Err(e) => return future::Either::Left(future::ready(Err(e))),
        };

        let is_query_safe_to_cache = match query.is_safe_to_cache_prepared(backend) {
            Ok(is_safe_to_cache) => is_safe_to_cache,
            Err(e) => return future::Either::Left(future::ready(Err(e))),
        };

        if !is_query_safe_to_cache {
            let sql = match cache_key.sql(&query, backend) {
                Ok(sql) => sql.into_owned(),
                Err(e) => return future::Either::Left(future::ready(Err(e))),
            };

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
                let sql = match entry.key().sql(&query, backend) {
                    Ok(sql) => sql.into_owned(),
                    Err(e) => return future::Either::Left(future::ready(Err(e))),
                };
                let metadata = metadata.to_vec();
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
