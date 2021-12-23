use std::pin::Pin;

use crate::stmt_cache::{PrepareCallback, StmtCache};
use crate::{
    AnsiTransactionManager, AsyncConnection, AsyncConnectionGatWorkaround, SimpleAsyncConnection,
};
use diesel::connection::MaybeCached;
use diesel::mysql::{Mysql, MysqlType};
use diesel::query_builder::{bind_collector::RawBytesBindCollector, QueryFragment, QueryId};
use diesel::result::{ConnectionError, ConnectionResult};
use diesel::QueryResult;
use futures::{Future, Stream, StreamExt, TryStreamExt};
use mysql_async::prelude::Queryable;
use mysql_async::{Opts, OptsBuilder, Params, Statement};

mod error_helper;
mod row;
mod serialize;

use self::error_helper::ErrorHelper;
use self::row::MysqlRow;
use self::serialize::ToSqlHelper;

pub struct AsyncMysqlConnection {
    conn: mysql_async::Conn,
    stmt_cache: StmtCache<Mysql, Statement>,
    transaction_manager: AnsiTransactionManager,
    last_stmt: Option<Statement>,
}

#[async_trait::async_trait]
impl SimpleAsyncConnection for AsyncMysqlConnection {
    async fn batch_execute(&mut self, query: &str) -> diesel::QueryResult<()> {
        Ok(self.conn.query_drop(query).await.map_err(ErrorHelper)?)
    }
}

impl<'a> AsyncConnectionGatWorkaround<'a, Mysql> for AsyncMysqlConnection {
    type Stream = Pin<Box<dyn Stream<Item = QueryResult<Self::Row>> + Send + 'a>>;

    type Row = MysqlRow;
}

#[async_trait::async_trait]
impl AsyncConnection for AsyncMysqlConnection {
    type Backend = Mysql;

    type TransactionManager = AnsiTransactionManager;

    async fn establish(database_url: &str) -> diesel::ConnectionResult<Self> {
        let opts = Opts::from_url(database_url)
            .map_err(|e| diesel::result::ConnectionError::InvalidConnectionUrl(e.to_string()))?;
        let builder = OptsBuilder::from_opts(opts).init(vec![
            "SET sql_mode=(SELECT CONCAT(@@sql_mode, ',PIPES_AS_CONCAT'))",
            "SET time_zone = '+00:00';",
            "SET character_set_client = 'utf8mb4'",
            "SET character_set_connection = 'utf8mb4'",
            "SET character_set_results = 'utf8mb4'",
        ]);

        let conn = mysql_async::Conn::new(builder).await.map_err(ErrorHelper)?;

        Ok(AsyncMysqlConnection {
            conn,
            stmt_cache: StmtCache::new(),
            transaction_manager: AnsiTransactionManager::default(),
            last_stmt: None,
        })
    }

    async fn execute(&mut self, query: &str) -> diesel::QueryResult<usize> {
        let stmt = self.conn.prep(query).await.map_err(ErrorHelper)?;
        self.conn
            .exec_drop(stmt, Params::Empty)
            .await
            .map_err(ErrorHelper)?;

        Ok(self.conn.affected_rows() as usize)
    }

    async fn load<'a, T>(
        &'a mut self,
        source: T,
    ) -> diesel::QueryResult<<Self as AsyncConnectionGatWorkaround<'a, Self::Backend>>::Stream>
    where
        T: diesel::query_builder::AsQuery + Send,
        T::Query: diesel::query_builder::QueryFragment<Self::Backend>
            + diesel::query_builder::QueryId
            + Send,
    {
        self.with_preapared_statement(source.as_query(), |conn, stmt, binds| async move {
            let res = conn.exec_iter(&*stmt, binds).await.map_err(ErrorHelper)?;

            let stream = res
                .stream_and_drop::<MysqlRow>()
                .await
                .map_err(|e| ErrorHelper(e))?
                .ok_or_else(|| {
                    diesel::result::Error::DeserializationError(Box::new(
                        diesel::result::UnexpectedEndOfRow,
                    ))
                })?
                .map_err(|e| diesel::result::Error::from(ErrorHelper(e)))
                .boxed();

            Ok(stream)
        })
        .await
    }

    async fn execute_returning_count<T>(&mut self, source: T) -> diesel::QueryResult<usize>
    where
        T: diesel::query_builder::QueryFragment<Self::Backend>
            + diesel::query_builder::QueryId
            + Send,
    {
        self.with_preapared_statement(source, |conn, stmt, binds| async move {
            conn.exec_drop(&*stmt, binds).await.map_err(ErrorHelper)?;
            Ok(conn.affected_rows() as usize)
        })
        .await
    }

    fn transaction_state(&mut self) -> &mut AnsiTransactionManager {
        &mut self.transaction_manager
    }
}

#[async_trait::async_trait]
impl PrepareCallback<Statement, MysqlType> for mysql_async::Conn {
    async fn prepare(
        &mut self,
        sql: &str,
        _metadata: &[MysqlType],
        _is_for_cache: diesel::connection::PrepareForCache,
    ) -> QueryResult<Statement> {
        Ok(self.prep(sql).await.map_err(ErrorHelper)?)
    }
}

impl AsyncMysqlConnection {
    pub async fn try_from(conn: mysql_async::Conn) -> ConnectionResult<Self> {
        let mut conn = AsyncMysqlConnection {
            conn,
            stmt_cache: StmtCache::new(),
            transaction_manager: AnsiTransactionManager::default(),
            last_stmt: None,
        };
        let setup_statements = vec![
            "SET sql_mode=(SELECT CONCAT(@@sql_mode, ',PIPES_AS_CONCAT'))",
            "SET time_zone = '+00:00';",
            "SET character_set_client = 'utf8mb4'",
            "SET character_set_connection = 'utf8mb4'",
            "SET character_set_results = 'utf8mb4'",
        ];

        for stmt in setup_statements {
            conn.execute(stmt)
                .await
                .map_err(ConnectionError::CouldntSetupConfiguration)?;
        }

        Ok(conn)
    }

    async fn with_preapared_statement<'a, T, F, R>(
        &'a mut self,
        query: T,
        callback: impl FnOnce(&'a mut mysql_async::Conn, &'a Statement, ToSqlHelper) -> F,
    ) -> QueryResult<R>
    where
        T: QueryFragment<Mysql> + QueryId + Send,
        F: Future<Output = QueryResult<R>>,
    {
        let mut bind_collector = RawBytesBindCollector::<Mysql>::new();
        query.collect_binds(&mut bind_collector, &mut ())?;

        let binds = bind_collector.binds;
        let metadata = bind_collector.metadata;

        let AsyncMysqlConnection {
            ref mut conn,
            ref mut stmt_cache,
            ref mut last_stmt,
            ..
        } = self;

        let conn = &mut *conn;

        let stmt = {
            let stmt = stmt_cache
                .cached_prepared_statement(query, &metadata, conn)
                .await?;
            stmt
        };

        let stmt = match stmt {
            MaybeCached::CannotCache(stmt) => {
                *last_stmt = Some(stmt);
                last_stmt.as_ref().unwrap()
            }
            MaybeCached::Cached(s) => s,
        };

        callback(&mut self.conn, stmt, ToSqlHelper { metadata, binds }).await
    }
}
