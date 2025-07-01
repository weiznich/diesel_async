use diesel::backend::Backend;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::Hash;

use super::{CacheSize, StatementCacheKey};

/// Indicates the cache key status
//
// This is a separate enum and not just `Option<Entry>`
// as we need to return the cache key for owner ship reasons
// if we don't have a cache at all
// cannot implement debug easily as StatementCacheKey is not Debug
pub enum LookupStatementResult<'a, DB, Statement>
where
    DB: Backend,
{
    /// The cache entry, either already populated or vacant
    /// in the later case the caller needs to prepare the
    /// statement and insert it into the cache
    CacheEntry(Entry<'a, StatementCacheKey<DB>, Statement>),
    /// This key should not be cached
    NoCache(StatementCacheKey<DB>),
}

/// Implement this trait, in order to control statement caching.
#[allow(unreachable_pub)]
pub trait StatementCacheStrategy<DB, Statement>: Send + 'static
where
    DB: Backend,
    StatementCacheKey<DB>: Hash + Eq,
{
    /// Returns which prepared statement cache size is implemented by this trait
    fn cache_size(&self) -> CacheSize;

    /// Returns whether or not the corresponding cache key is already cached
    fn lookup_statement(
        &mut self,
        key: StatementCacheKey<DB>,
    ) -> LookupStatementResult<'_, DB, Statement>;
}

/// Cache all (safe) statements for as long as connection is alive.
#[allow(missing_debug_implementations, unreachable_pub)]
pub struct WithCacheStrategy<DB, Statement>
where
    DB: Backend,
{
    cache: HashMap<StatementCacheKey<DB>, Statement>,
}

impl<DB, Statement> Default for WithCacheStrategy<DB, Statement>
where
    DB: Backend,
{
    fn default() -> Self {
        Self {
            cache: Default::default(),
        }
    }
}

impl<DB, Statement> StatementCacheStrategy<DB, Statement> for WithCacheStrategy<DB, Statement>
where
    DB: Backend + 'static,
    StatementCacheKey<DB>: Hash + Eq,
    DB::TypeMetadata: Send + Clone,
    DB::QueryBuilder: Default,
    Statement: Send + 'static,
{
    fn lookup_statement(
        &mut self,
        entry: StatementCacheKey<DB>,
    ) -> LookupStatementResult<'_, DB, Statement> {
        LookupStatementResult::CacheEntry(self.cache.entry(entry))
    }

    fn cache_size(&self) -> CacheSize {
        CacheSize::Unbounded
    }
}

/// No statements will be cached,
#[allow(missing_debug_implementations, unreachable_pub)]
#[derive(Clone, Copy, Default)]
pub struct WithoutCacheStrategy {}

impl<DB, Statement> StatementCacheStrategy<DB, Statement> for WithoutCacheStrategy
where
    DB: Backend,
    StatementCacheKey<DB>: Hash + Eq,
    DB::TypeMetadata: Clone,
    DB::QueryBuilder: Default,
    Statement: 'static,
{
    fn lookup_statement(
        &mut self,
        entry: StatementCacheKey<DB>,
    ) -> LookupStatementResult<'_, DB, Statement> {
        LookupStatementResult::NoCache(entry)
    }

    fn cache_size(&self) -> CacheSize {
        CacheSize::Disabled
    }
}

#[allow(dead_code)]
#[cfg(test)]
mod testing_utils {
    use diesel::connection::{Instrumentation, InstrumentationEvent};
    use std::sync::{Arc, Mutex};

    pub struct RecordCacheEvents {
        pub list: Arc<Mutex<Vec<String>>>,
    }

    impl Instrumentation for RecordCacheEvents {
        fn on_connection_event(&mut self, event: InstrumentationEvent<'_>) {
            if let InstrumentationEvent::CacheQuery { sql, .. } = event {
                let mut list = self.list.lock().unwrap();
                list.push(sql.to_owned());
            }
        }
    }

    pub fn count_cache_calls(list: &Mutex<Vec<String>>) -> usize {
        let lock = list.lock().unwrap();
        lock.len()
    }
}

#[cfg(test)]
#[cfg(feature = "postgres")]
mod tests_pg {
    use std::sync::{Arc, Mutex};

    use crate::{AsyncConnection, AsyncPgConnection, CacheSize, RunQueryDsl};
    use diesel::dsl::sql;
    use diesel::insertable::Insertable;
    use diesel::pg::Pg;
    use diesel::sql_types::{Integer, VarChar};
    use diesel::table;
    use diesel::{ExpressionMethods, IntoSql, QueryDsl};

    use super::testing_utils::{count_cache_calls, RecordCacheEvents};

    diesel::define_sql_function! {
        fn lower(x: VarChar) -> VarChar;
    }

    table! {
        users {
            id -> Integer,
            name -> Text,
        }
    }

    pub async fn connection() -> (AsyncPgConnection, Arc<Mutex<Vec<String>>>) {
        let mut conn = AsyncPgConnection::establish(&std::env::var("DATABASE_URL").unwrap())
            .await
            .unwrap();
        let list = Arc::new(Mutex::new(Vec::new()));
        conn.set_instrumentation(RecordCacheEvents { list: list.clone() });
        (conn, list)
    }

    #[tokio::test]
    async fn prepared_statements_are_cached() {
        let (connection, list) = &mut connection().await;

        let query = diesel::select(1.into_sql::<Integer>());

        assert_eq!(Ok(1), query.get_result(connection).await);
        assert_eq!(1, count_cache_calls(list));
        assert_eq!(Ok(1), query.get_result(connection).await);
        assert_eq!(1, count_cache_calls(list));
    }

    #[tokio::test]
    async fn queries_with_identical_sql_but_different_types_are_cached_separately() {
        let (connection, list) = &mut connection().await;

        let query = diesel::select(1.into_sql::<Integer>());
        let query2 = diesel::select("hi".into_sql::<VarChar>());

        assert_eq!(Ok(1), query.get_result(connection).await);
        assert_eq!(1, count_cache_calls(list));
        assert_eq!(Ok("hi".to_string()), query2.get_result(connection).await);
        assert_eq!(2, count_cache_calls(list));
    }

    #[tokio::test]
    async fn queries_with_identical_types_and_sql_but_different_bind_types_are_cached_separately() {
        let (connection, list) = &mut connection().await;

        let query = diesel::select(1.into_sql::<Integer>()).into_boxed::<Pg>();
        let query2 = diesel::select("hi".into_sql::<VarChar>()).into_boxed::<Pg>();

        assert_eq!(Ok(1), query.get_result(connection).await);
        assert_eq!(1, count_cache_calls(list));
        assert_eq!(Ok("hi".to_string()), query2.get_result(connection).await);
        assert_eq!(2, count_cache_calls(list));
    }

    #[tokio::test]
    async fn queries_with_identical_types_and_binds_but_different_sql_are_cached_separately() {
        let (connection, list) = &mut connection().await;

        let hi = "HI".into_sql::<VarChar>();
        let query = diesel::select(hi).into_boxed::<Pg>();
        let query2 = diesel::select(lower(hi)).into_boxed::<Pg>();

        assert_eq!(Ok("HI".to_string()), query.get_result(connection).await);
        assert_eq!(1, count_cache_calls(list));
        assert_eq!(Ok("hi".to_string()), query2.get_result(connection).await);
        assert_eq!(2, count_cache_calls(list));
    }

    #[tokio::test]
    async fn queries_with_sql_literal_nodes_are_not_cached() {
        let (connection, list) = &mut connection().await;
        let query = diesel::select(sql::<Integer>("1"));

        assert_eq!(Ok(1), query.get_result(connection).await);
        assert_eq!(0, count_cache_calls(list));
    }

    #[tokio::test]
    async fn inserts_from_select_are_cached() {
        let (connection, list) = &mut connection().await;
        connection.begin_test_transaction().await.unwrap();

        diesel::sql_query(
            "CREATE TEMPORARY TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
        )
        .execute(connection)
        .await
        .unwrap();

        let query = users::table.filter(users::id.eq(42));
        let insert = query
            .insert_into(users::table)
            .into_columns((users::id, users::name));
        assert!(insert.execute(connection).await.is_ok());
        assert_eq!(1, count_cache_calls(list));

        let query = users::table.filter(users::id.eq(42)).into_boxed();
        let insert = query
            .insert_into(users::table)
            .into_columns((users::id, users::name));
        assert!(insert.execute(connection).await.is_ok());
        assert_eq!(2, count_cache_calls(list));
    }

    #[tokio::test]
    async fn single_inserts_are_cached() {
        let (connection, list) = &mut connection().await;
        connection.begin_test_transaction().await.unwrap();

        diesel::sql_query(
            "CREATE TEMPORARY TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
        )
        .execute(connection)
        .await
        .unwrap();

        let insert =
            diesel::insert_into(users::table).values((users::id.eq(42), users::name.eq("Foo")));

        assert!(insert.execute(connection).await.is_ok());
        assert_eq!(1, count_cache_calls(list));
    }

    #[tokio::test]
    async fn dynamic_batch_inserts_are_not_cached() {
        let (connection, list) = &mut connection().await;
        connection.begin_test_transaction().await.unwrap();

        diesel::sql_query(
            "CREATE TEMPORARY TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
        )
        .execute(connection)
        .await
        .unwrap();

        let insert = diesel::insert_into(users::table)
            .values(vec![(users::id.eq(42), users::name.eq("Foo"))]);

        assert!(insert.execute(connection).await.is_ok());
        assert_eq!(0, count_cache_calls(list));
    }

    #[tokio::test]
    async fn static_batch_inserts_are_cached() {
        let (connection, list) = &mut connection().await;
        connection.begin_test_transaction().await.unwrap();

        diesel::sql_query(
            "CREATE TEMPORARY TABLE users(id INTEGER PRIMARY KEY, name TEXT NOT NULL);",
        )
        .execute(connection)
        .await
        .unwrap();

        let insert =
            diesel::insert_into(users::table).values([(users::id.eq(42), users::name.eq("Foo"))]);

        assert!(insert.execute(connection).await.is_ok());
        assert_eq!(1, count_cache_calls(list));
    }

    #[tokio::test]
    async fn queries_containing_in_with_vec_are_cached() {
        let (connection, list) = &mut connection().await;
        let one_as_expr = 1.into_sql::<Integer>();
        let query = diesel::select(one_as_expr.eq_any(vec![1, 2, 3]));

        assert_eq!(Ok(true), query.get_result(connection).await);
        assert_eq!(1, count_cache_calls(list));
    }

    #[tokio::test]
    async fn disabling_the_cache_works() {
        let (connection, list) = &mut connection().await;
        connection.set_prepared_statement_cache_size(CacheSize::Disabled);

        let query = diesel::select(1.into_sql::<Integer>());

        assert_eq!(Ok(1), query.get_result(connection).await);
        assert_eq!(0, count_cache_calls(list));
        assert_eq!(Ok(1), query.get_result(connection).await);
        assert_eq!(0, count_cache_calls(list));
    }
}
