use crate::stmt_cache::{PrepareCallback, StmtCache};
use crate::{AnsiTransactionManager, AsyncConnection, SimpleAsyncConnection};
use diesel::connection::statement_cache::{MaybeCached, StatementCacheKey};
use diesel::mysql::{Mysql, MysqlQueryBuilder, MysqlType};
use diesel::query_builder::QueryBuilder;
use diesel::query_builder::{bind_collector::RawBytesBindCollector, QueryFragment, QueryId};
use diesel::result::{ConnectionError, ConnectionResult};
use diesel::QueryResult;
use futures_util::future::BoxFuture;
use futures_util::stream::{self, BoxStream};
use futures_util::{Future, FutureExt, StreamExt, TryStreamExt};
use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Statement};

mod error_helper;
mod row;
mod serialize;

use self::error_helper::ErrorHelper;
use self::row::MysqlRow;
use self::serialize::ToSqlHelper;

/// A connection to a MySQL database. Connection URLs should be in the form
/// `mysql://[user[:password]@]host/database_name`
pub struct AsyncMysqlConnection {
    conn: mysql_async::Conn,
    stmt_cache: StmtCache<Mysql, Statement>,
    transaction_manager: AnsiTransactionManager,
}

#[async_trait::async_trait]
impl SimpleAsyncConnection for AsyncMysqlConnection {
    async fn batch_execute(&mut self, query: &str) -> diesel::QueryResult<()> {
        Ok(self.conn.query_drop(query).await.map_err(ErrorHelper)?)
    }
}

const CONNECTION_SETUP_QUERIES: &[&str] = &[
    "SET time_zone = '+00:00';",
    "SET character_set_client = 'utf8mb4'",
    "SET character_set_connection = 'utf8mb4'",
    "SET character_set_results = 'utf8mb4'",
];

#[async_trait::async_trait]
impl AsyncConnection for AsyncMysqlConnection {
    type ExecuteFuture<'conn, 'query> = BoxFuture<'conn, QueryResult<usize>>;
    type LoadFuture<'conn, 'query> = BoxFuture<'conn, QueryResult<Self::Stream<'conn, 'query>>>;
    type Stream<'conn, 'query> = BoxStream<'conn, QueryResult<Self::Row<'conn, 'query>>>;
    type Row<'conn, 'query> = MysqlRow;
    type Backend = Mysql;

    type TransactionManager = AnsiTransactionManager;

    async fn establish(database_url: &str) -> diesel::ConnectionResult<Self> {
        let opts = Opts::from_url(database_url)
            .map_err(|e| diesel::result::ConnectionError::InvalidConnectionUrl(e.to_string()))?;
        let builder = OptsBuilder::from_opts(opts)
            .init(CONNECTION_SETUP_QUERIES.to_vec())
            .stmt_cache_size(0) // We have our own cache
            .client_found_rows(true); // This allows a consistent behavior between MariaDB/MySQL and PostgreSQL (and is already set in `diesel`)

        let conn = mysql_async::Conn::new(builder).await.map_err(ErrorHelper)?;

        Ok(AsyncMysqlConnection {
            conn,
            stmt_cache: StmtCache::new(),
            transaction_manager: AnsiTransactionManager::default(),
        })
    }

    fn load<'conn, 'query, T>(&'conn mut self, source: T) -> Self::LoadFuture<'conn, 'query>
    where
        T: diesel::query_builder::AsQuery,
        T::Query: diesel::query_builder::QueryFragment<Self::Backend>
            + diesel::query_builder::QueryId
            + 'query,
    {
        self.with_prepared_statement(source.as_query(), |conn, stmt, binds| async move {
            let stmt_for_exec = match stmt {
                MaybeCached::Cached(ref s) => (*s).clone(),
                MaybeCached::CannotCache(ref s) => s.clone(),
                _ => todo!(),
            };

            let (tx, rx) = futures_channel::mpsc::channel(0);

            let yielder = async move {
                let r = Self::poll_result_stream(conn, stmt_for_exec, binds, tx).await;
                // We need to close any non-cached statement explicitly here as otherwise
                // we might error out on too many open statements. See https://github.com/weiznich/diesel_async/issues/26
                // for details
                //
                // This might be problematic for cases where the stream is dropped before the end is reached
                //
                // Such behaviour might happen if users:
                // * Just drop the future/stream after polling at least once (timeouts!!)
                // * Users only fetch a fixed number of elements from the stream
                //
                // For now there is not really a good solution to this problem as this would require something like async drop
                // (and even with async drop that would be really hard to solve due to the involved lifetimes)
                if let MaybeCached::CannotCache(stmt) = stmt {
                    conn.close(stmt).await.map_err(ErrorHelper)?;
                }
                r
            };

            let fake_stream = stream::once(yielder).filter_map(|e: QueryResult<()>| async move {
                if let Err(e) = e {
                    Some(Err(e))
                } else {
                    None
                }
            });

            let stream = stream::select(fake_stream, rx).boxed();

            Ok(stream)
        })
        .boxed()
    }

    fn execute_returning_count<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> Self::ExecuteFuture<'conn, 'query>
    where
        T: diesel::query_builder::QueryFragment<Self::Backend>
            + diesel::query_builder::QueryId
            + 'query,
    {
        self.with_prepared_statement(source, |conn, stmt, binds| async move {
            conn.exec_drop(&*stmt, binds).await.map_err(ErrorHelper)?;
            // We need to close any non-cached statement explicitly here as otherwise
            // we might error out on too many open statements. See https://github.com/weiznich/diesel_async/issues/26
            // for details
            //
            // This might be problematic for cases where the stream is dropped before the end is reached
            //
            // Such behaviour might happen if users:
            // * Just drop the future after polling at least once (timeouts!!)
            //
            // For now there is not really a good solution to this problem as this would require something like async drop
            // (and even with async drop that would be really hard to solve due to the involved lifetimes)
            if let MaybeCached::CannotCache(stmt) = stmt {
                conn.close(stmt).await.map_err(ErrorHelper)?;
            }
            Ok(conn.affected_rows() as usize)
        })
    }

    fn transaction_state(&mut self) -> &mut AnsiTransactionManager {
        &mut self.transaction_manager
    }
}

#[inline(always)]
fn update_transaction_manager_status<T>(
    query_result: QueryResult<T>,
    transaction_manager: &mut AnsiTransactionManager,
) -> QueryResult<T> {
    if let Err(diesel::result::Error::DatabaseError(
        diesel::result::DatabaseErrorKind::SerializationFailure,
        _,
    )) = query_result
    {
        transaction_manager
            .status
            .set_requires_rollback_maybe_up_to_top_level(true)
    }
    query_result
}

#[async_trait::async_trait]
impl PrepareCallback<Statement, MysqlType> for &'_ mut mysql_async::Conn {
    async fn prepare(
        self,
        sql: &str,
        _metadata: &[MysqlType],
        _is_for_cache: diesel::connection::statement_cache::PrepareForCache,
    ) -> QueryResult<(Statement, Self)> {
        let s = self.prep(sql).await.map_err(ErrorHelper)?;
        Ok((s, self))
    }
}

impl AsyncMysqlConnection {
    /// Wrap an existing [`mysql_async::Conn`] into a async diesel mysql connection
    ///
    /// This function constructs a new `AsyncMysqlConnection` based on an existing
    /// [`mysql_async::Conn]`.
    pub async fn try_from(conn: mysql_async::Conn) -> ConnectionResult<Self> {
        use crate::run_query_dsl::RunQueryDsl;
        let mut conn = AsyncMysqlConnection {
            conn,
            stmt_cache: StmtCache::new(),
            transaction_manager: AnsiTransactionManager::default(),
        };

        for stmt in CONNECTION_SETUP_QUERIES {
            diesel::sql_query(*stmt)
                .execute(&mut conn)
                .await
                .map_err(ConnectionError::CouldntSetupConfiguration)?;
        }

        Ok(conn)
    }

    fn with_prepared_statement<'conn, T, F, R>(
        &'conn mut self,
        query: T,
        callback: impl (FnOnce(&'conn mut mysql_async::Conn, MaybeCached<'conn, Statement>, ToSqlHelper) -> F)
            + Send
            + 'conn,
    ) -> BoxFuture<'conn, QueryResult<R>>
    where
        R: Send + 'conn,
        T: QueryFragment<Mysql> + QueryId,
        F: Future<Output = QueryResult<R>> + Send,
    {
        let mut bind_collector = RawBytesBindCollector::<Mysql>::new();
        let bind_collector = query
            .collect_binds(&mut bind_collector, &mut (), &Mysql)
            .map(|()| bind_collector);

        let AsyncMysqlConnection {
            ref mut conn,
            ref mut stmt_cache,
            ref mut transaction_manager,
            ..
        } = self;

        let is_safe_to_cache_prepared = query.is_safe_to_cache_prepared(&Mysql);
        let mut qb = MysqlQueryBuilder::new();
        let sql = query.to_sql(&mut qb, &Mysql).map(|()| qb.finish());
        let query_id = T::query_id();

        async move {
            let RawBytesBindCollector {
                metadata, binds, ..
            } = bind_collector?;
            let is_safe_to_cache_prepared = is_safe_to_cache_prepared?;
            let sql = sql?;
            let cache_key = if let Some(query_id) = query_id {
                StatementCacheKey::Type(query_id)
            } else {
                StatementCacheKey::Sql {
                    sql: sql.clone(),
                    bind_types: metadata.clone(),
                }
            };

            let (stmt, conn) = stmt_cache
                .cached_prepared_statement(
                    cache_key,
                    sql,
                    is_safe_to_cache_prepared,
                    &metadata,
                    conn,
                )
                .await?;
            update_transaction_manager_status(
                callback(conn, stmt, ToSqlHelper { metadata, binds }).await,
                transaction_manager,
            )
        }
        .boxed()
    }

    async fn poll_result_stream(
        conn: &mut mysql_async::Conn,
        stmt_for_exec: mysql_async::Statement,
        binds: ToSqlHelper,
        mut tx: futures_channel::mpsc::Sender<QueryResult<MysqlRow>>,
    ) -> QueryResult<()> {
        use futures_util::sink::SinkExt;
        let res = conn
            .exec_iter(stmt_for_exec, binds)
            .await
            .map_err(ErrorHelper)?;

        let mut stream = res
            .stream_and_drop::<MysqlRow>()
            .await
            .map_err(ErrorHelper)?
            .ok_or_else(|| {
                diesel::result::Error::DeserializationError(Box::new(
                    diesel::result::UnexpectedEndOfRow,
                ))
            })?
            .map_err(|e| diesel::result::Error::from(ErrorHelper(e)));

        while let Some(row) = stream.next().await {
            let row = row?;
            tx.send(Ok(row))
                .await
                .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))?;
        }

        Ok(())
    }
}

#[cfg(any(
    feature = "deadpool",
    feature = "bb8",
    feature = "mobc",
    feature = "r2d2"
))]
impl crate::pooled_connection::PoolableConnection for AsyncMysqlConnection {}

#[cfg(test)]
mod tests {
    use crate::RunQueryDsl;
    mod diesel_async {
        pub use crate::*;
    }
    include!("../doctest_setup.rs");

    #[tokio::test]
    async fn check_statements_are_dropped() {
        use self::schema::users;

        let mut conn = establish_connection().await;
        // we cannot set a lower limit here without admin privileges
        // which makes this test really slow
        let stmt_count = 16382 + 10;

        for i in 0..stmt_count {
            diesel::insert_into(users::table)
                .values(Some(users::name.eq(format!("User{i}"))))
                .execute(&mut conn)
                .await
                .unwrap();
        }

        #[derive(QueryableByName)]
        #[diesel(table_name = users)]
        #[allow(dead_code)]
        struct User {
            id: i32,
            name: String,
        }

        for i in 0..stmt_count {
            diesel::sql_query("SELECT id, name FROM users WHERE name = ?")
                .bind::<diesel::sql_types::Text, _>(format!("User{i}"))
                .load::<User>(&mut conn)
                .await
                .unwrap();
        }
    }
}
