use crate::stmt_cache::{PrepareCallback, StmtCache};
use crate::{
    AnsiTransactionManager, AsyncConnection, AsyncConnectionGatWorkaround, SimpleAsyncConnection,
};
use diesel::connection::statement_cache::MaybeCached;
use diesel::mysql::{Mysql, MysqlType};
use diesel::query_builder::{bind_collector::RawBytesBindCollector, QueryFragment, QueryId};
use diesel::result::{ConnectionError, ConnectionResult};
use diesel::QueryResult;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{Future, FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Statement};

mod error_helper;
mod row;
mod serialize;

use self::error_helper::ErrorHelper;
use self::row::MysqlRow;
use self::serialize::ToSqlHelper;

/// A connection to a MySQL database. Connection URLs should be in the form
/// `mysql://[user[:password]@]host/database_name`.
///
/// See [`mysql_async::Opts`] for more available connection url queries. Notice that
/// param `stmt_cache_size` will be ignored.
pub struct AsyncMysqlConnection {
    conn: mysql_async::Conn,
    stmt_cache: StmtCache<Mysql, Statement>,
    transaction_manager: AnsiTransactionManager,
    // a workaround so that for non-cacheable stmts we still get a `&'conn Statement`.
    last_stmt: Option<Statement>,
}

#[async_trait::async_trait]
impl SimpleAsyncConnection for AsyncMysqlConnection {
    async fn batch_execute(&mut self, query: &str) -> diesel::QueryResult<()> {
        Ok(self.conn.query_drop(query).await.map_err(ErrorHelper)?)
    }
}

impl<'conn, 'query> AsyncConnectionGatWorkaround<'conn, 'query, Mysql> for AsyncMysqlConnection {
    type ExecuteFuture = BoxFuture<'conn, QueryResult<usize>>;
    type LoadFuture = BoxFuture<'conn, QueryResult<Self::Stream>>;
    type Stream = BoxStream<'conn, QueryResult<Self::Row>>;

    type Row = MysqlRow;
}

const CONNECTION_SETUP_QUERIES: &'static [&'static str] = &[
    "SET sql_mode=(SELECT CONCAT(@@sql_mode, ',PIPES_AS_CONCAT'))",
    "SET time_zone = '+00:00';",
    "SET character_set_client = 'utf8mb4'",
    "SET character_set_connection = 'utf8mb4'",
    "SET character_set_results = 'utf8mb4'",
];

#[async_trait::async_trait]
impl AsyncConnection for AsyncMysqlConnection {
    type Backend = Mysql;

    type TransactionManager = AnsiTransactionManager;

    async fn establish(database_url: &str) -> diesel::ConnectionResult<Self> {
        let opts = Opts::from_url(database_url)
            .map_err(|e| diesel::result::ConnectionError::InvalidConnectionUrl(e.to_string()))?;
        let builder = OptsBuilder::from_opts(opts)
            .init(CONNECTION_SETUP_QUERIES.to_vec())
            .stmt_cache_size(0); // We have our own cache

        let conn = mysql_async::Conn::new(builder).await.map_err(ErrorHelper)?;

        Ok(AsyncMysqlConnection {
            conn,
            stmt_cache: StmtCache::new(),
            transaction_manager: AnsiTransactionManager::default(),
            last_stmt: None,
        })
    }

    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> <Self as AsyncConnectionGatWorkaround<'conn, 'query, Self::Backend>>::LoadFuture
    where
        T: diesel::query_builder::AsQuery + Send,
        T::Query: diesel::query_builder::QueryFragment<Self::Backend>
            + diesel::query_builder::QueryId
            + Send
            + 'query,
    {
        self.with_prepared_statement(source.as_query(), |conn, stmt, binds| async move {
            let res = conn.exec_iter(stmt, binds).await.map_err(ErrorHelper)?;

            let stream = res
                .stream_and_drop::<MysqlRow>()
                .await
                .map_err(ErrorHelper)?
                .ok_or_else(|| {
                    diesel::result::Error::DeserializationError(Box::new(
                        diesel::result::UnexpectedEndOfRow,
                    ))
                })?
                .map_err(|e| diesel::result::Error::from(ErrorHelper(e)))
                .boxed();

            Ok(stream)
        })
        .boxed()
    }

    fn execute_returning_count<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> <Self as AsyncConnectionGatWorkaround<'conn, 'query, Self::Backend>>::ExecuteFuture
    where
        T: diesel::query_builder::QueryFragment<Self::Backend>
            + diesel::query_builder::QueryId
            + Send
            + 'query,
    {
        self.with_prepared_statement(source, |conn, stmt, binds| async move {
            conn.exec_drop(stmt, binds).await.map_err(ErrorHelper)?;
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
            .set_top_level_transaction_requires_rollback()
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
            last_stmt: None,
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
        callback: impl (FnOnce(&'conn mut mysql_async::Conn, &'conn Statement, ToSqlHelper) -> F)
            + Send
            + 'conn,
    ) -> BoxFuture<'conn, QueryResult<R>>
    where
        R: Send + 'conn,
        T: QueryFragment<Mysql> + QueryId + Send,
        F: Future<Output = QueryResult<R>> + Send,
    {
        let mut bind_collector = RawBytesBindCollector::<Mysql>::new();
        if let Err(e) = query.collect_binds(&mut bind_collector, &mut (), &Mysql) {
            return futures::future::ready(Err(e)).boxed();
        }

        let binds = bind_collector.binds;
        let metadata = bind_collector.metadata;

        let AsyncMysqlConnection {
            ref mut conn,
            ref mut stmt_cache,
            ref mut last_stmt,
            ref mut transaction_manager,
            ..
        } = self;

        let stmt = stmt_cache.cached_prepared_statement(query, &metadata, conn, &Mysql);

        stmt.and_then(|(stmt, conn)| async move {
            let stmt = match stmt {
                MaybeCached::CannotCache(stmt) => {
                    if let Some(last_stmt) = std::mem::take(last_stmt) {
                        conn.close(last_stmt).await.map_err(ErrorHelper)?;
                    }
                    *last_stmt = Some(stmt);
                    // SAFETY: we just set last_stmt to Some.
                    last_stmt.as_ref().unwrap()
                }
                MaybeCached::Cached(s) => s,
                _ => unreachable!("We've opted into breaking diesel changes and want to know if things break because someone added a new variant here")
            };
            update_transaction_manager_status(callback(conn, stmt, ToSqlHelper{metadata, binds}).await, transaction_manager)
        }).boxed()
    }
}

#[cfg(any(feature = "deadpool", feature = "bb8", feature = "mobc"))]
impl crate::pooled_connection::PoolableConnection for AsyncMysqlConnection {}
