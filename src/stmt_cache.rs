use std::collections::HashMap;
use std::hash::Hash;

use diesel::backend::Backend;
use diesel::connection::{MaybeCached, PrepareForCache, StatementCacheKey};
use diesel::query_builder::{QueryFragment, QueryId};
use diesel::QueryResult;

pub struct StmtCache<DB: Backend, S> {
    cache: HashMap<StatementCacheKey<DB>, S>,
}

#[async_trait::async_trait]
pub trait PrepareCallback<S, M> {
    async fn prepare(
        &mut self,
        sql: &str,
        metadata: &[M],
        is_for_cache: PrepareForCache,
    ) -> QueryResult<S>;
}

impl<S, DB: Backend> StmtCache<DB, S> {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    pub async fn cached_prepared_statement<'a, T, F>(
        &'a mut self,
        query: T,
        metadata: &[DB::TypeMetadata],
        prepare_fn: &mut F,
    ) -> QueryResult<MaybeCached<'_, S>>
    where
        DB::QueryBuilder: Default,
        DB::TypeMetadata: Clone,
        T: QueryFragment<DB> + QueryId + Send,
        F: PrepareCallback<S, DB::TypeMetadata>,
        StatementCacheKey<DB>: Hash + Eq,
    {
        use std::collections::hash_map::Entry::{Occupied, Vacant};

        let cache_key = StatementCacheKey::for_source(&query, &metadata)?;

        if !query.is_safe_to_cache_prepared()? {
            let sql = cache_key.sql(&query)?;

            let stmt = prepare_fn
                .prepare(&*sql, metadata, PrepareForCache::No)
                .await?;
            return Ok(MaybeCached::CannotCache(stmt));
        }

        let cached_result = match self.cache.entry(cache_key) {
            Occupied(entry) => entry.into_mut(),
            Vacant(entry) => {
                let statement = {
                    let sql = entry.key().sql(&query)?;
                    prepare_fn
                        .prepare(&*sql, metadata, PrepareForCache::Yes)
                        .await?
                };

                entry.insert(statement)
            }
        };

        Ok(MaybeCached::Cached(cached_result))
    }
}
