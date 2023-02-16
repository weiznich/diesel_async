//! Provides types and functions related to working with PostgreSQL
//!
//! Much of this module is re-exported from database agnostic locations.
//! However, if you are writing code specifically to extend Diesel on
//! PostgreSQL, you may need to work with this module directly.

use self::error_helper::ErrorHelper;
use self::row::PgRow;
use self::serialize::ToSqlHelper;
use crate::stmt_cache::{PrepareCallback, StmtCache};
use crate::{AnsiTransactionManager, AsyncConnection, SimpleAsyncConnection};
use diesel::connection::statement_cache::PrepareForCache;
use diesel::pg::{
    FailedToLookupTypeError, PgMetadataCache, PgMetadataCacheKey, PgMetadataLookup, PgTypeMetadata,
};
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
use diesel::{ConnectionError, ConnectionResult, QueryResult};
use futures_util::future::BoxFuture;
use futures_util::lock::Mutex;
use futures_util::stream::{BoxStream, TryStreamExt};
use futures_util::{Future, FutureExt, StreamExt};
use std::borrow::Cow;
use std::sync::Arc;
use tokio_postgres::types::ToSql;
use tokio_postgres::types::Type;
use tokio_postgres::Statement;

pub use self::transaction_builder::TransactionBuilder;

mod error_helper;
mod row;
mod serialize;
mod transaction_builder;

/// A connection to a PostgreSQL database.
///
/// Connection URLs should be in the form
/// `postgres://[user[:password]@]host/database_name`
///
/// Checkout the documentation of the [tokio_postgres]
/// crate for details about the format
///
/// [tokio_postgres]: https://docs.rs/tokio-postgres/0.7.6/tokio_postgres/config/struct.Config.html#url
///
/// This connection supports *pipelined* requests. Pipelining can improve performance in use cases in which multiple,
/// independent queries need to be executed. In a traditional workflow, each query is sent to the server after the
/// previous query completes. In contrast, pipelining allows the client to send all of the queries to the server up
/// front, minimizing time spent by one side waiting for the other to finish sending data:
///
/// ```not_rust
///             Sequential                              Pipelined
/// | Client         | Server          |    | Client         | Server          |
/// |----------------|-----------------|    |----------------|-----------------|
/// | send query 1   |                 |    | send query 1   |                 |
/// |                | process query 1 |    | send query 2   | process query 1 |
/// | receive rows 1 |                 |    | send query 3   | process query 2 |
/// | send query 2   |                 |    | receive rows 1 | process query 3 |
/// |                | process query 2 |    | receive rows 2 |                 |
/// | receive rows 2 |                 |    | receive rows 3 |                 |
/// | send query 3   |                 |
/// |                | process query 3 |
/// | receive rows 3 |                 |
/// ```
///
/// In both cases, the PostgreSQL server is executing the queries **sequentially** - pipelining just allows both sides of
/// the connection to work concurrently when possible.
///
/// Pipelining happens automatically when futures are polled concurrently (for example, by using the futures `join`
/// combinator):
///
/// ```rust
/// # include!("../doctest_setup.rs");
/// #
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// #     run_test().await.unwrap();
/// # }
/// #
/// # async fn run_test() -> QueryResult<()> {
/// #     use diesel::sql_types::{Text, Integer};
/// #     let conn = &mut establish_connection().await;
///       let q1 = diesel::select(1_i32.into_sql::<Integer>());
///       let q2 = diesel::select(2_i32.into_sql::<Integer>());
///
///       // construct multiple futures for different queries
///       let f1 = q1.get_result::<i32>(conn);
///       let f2 = q2.get_result::<i32>(conn);
///
///       // wait on both results
///       let res = futures_util::try_join!(f1, f2)?;
///
///       assert_eq!(res.0, 1);
///       assert_eq!(res.1, 2);
///       # Ok(())
/// # }
pub struct AsyncPgConnection {
    conn: Arc<tokio_postgres::Client>,
    stmt_cache: Arc<Mutex<StmtCache<diesel::pg::Pg, Statement>>>,
    transaction_state: Arc<Mutex<AnsiTransactionManager>>,
    metadata_cache: Arc<Mutex<Option<PgMetadataCache>>>,
}

#[async_trait::async_trait]
impl SimpleAsyncConnection for AsyncPgConnection {
    async fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        Ok(self.conn.batch_execute(query).await.map_err(ErrorHelper)?)
    }
}

#[async_trait::async_trait]
impl AsyncConnection for AsyncPgConnection {
    type LoadFuture<'conn, 'query> = BoxFuture<'query, QueryResult<Self::Stream<'conn, 'query>>>;
    type ExecuteFuture<'conn, 'query> = BoxFuture<'query, QueryResult<usize>>;
    type Stream<'conn, 'query> = BoxStream<'static, QueryResult<PgRow>>;
    type Row<'conn, 'query> = PgRow;
    type Backend = diesel::pg::Pg;
    type TransactionManager = AnsiTransactionManager;

    async fn establish(database_url: &str) -> ConnectionResult<Self> {
        let (client, connection) = tokio_postgres::connect(database_url, tokio_postgres::NoTls)
            .await
            .map_err(ErrorHelper)?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {e}");
            }
        });
        Self::try_from(client).await
    }

    fn load<'conn, 'query, T>(&'conn mut self, source: T) -> Self::LoadFuture<'conn, 'query>
    where
        T: AsQuery + Send + 'query,
        T::Query: QueryFragment<Self::Backend> + QueryId + Send + 'query,
    {
        let conn = self.conn.clone();
        let stmt_cache = self.stmt_cache.clone();
        let metadata_cache = self.metadata_cache.clone();
        let tm = self.transaction_state.clone();
        let query = source.as_query();
        Self::with_prepared_statement(
            conn,
            stmt_cache,
            metadata_cache,
            tm,
            query,
            |conn, stmt, binds| async move {
                let res = conn.query_raw(&stmt, binds).await.map_err(ErrorHelper)?;

                Ok(res
                    .map_err(|e| diesel::result::Error::from(ErrorHelper(e)))
                    .map_ok(PgRow::new)
                    .boxed())
            },
        )
        .boxed()
    }

    fn execute_returning_count<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> Self::ExecuteFuture<'conn, 'query>
    where
        T: QueryFragment<Self::Backend> + QueryId + Send + 'query,
    {
        Self::with_prepared_statement(
            self.conn.clone(),
            self.stmt_cache.clone(),
            self.metadata_cache.clone(),
            self.transaction_state.clone(),
            source,
            |conn, stmt, binds| async move {
                let binds = binds
                    .iter()
                    .map(|b| b as &(dyn ToSql + Sync))
                    .collect::<Vec<_>>();

                let res = tokio_postgres::Client::execute(&conn, &stmt, &binds as &[_])
                    .await
                    .map_err(ErrorHelper)?;
                Ok(res as usize)
            },
        )
        .boxed()
    }

    fn transaction_state(&mut self) -> &mut AnsiTransactionManager {
        // there should be no other pending future when this is called
        // that means there is only one instance of this arc and
        // we can simply access the inner data
        if let Some(tm) = Arc::get_mut(&mut self.transaction_state) {
            tm.get_mut()
        } else {
            panic!("Cannot access shared transaction state")
        }
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
impl PrepareCallback<Statement, PgTypeMetadata> for Arc<tokio_postgres::Client> {
    async fn prepare(
        self,
        sql: &str,
        metadata: &[PgTypeMetadata],
        _is_for_cache: PrepareForCache,
    ) -> QueryResult<(Statement, Self)> {
        let bind_types = metadata
            .iter()
            .map(type_from_oid)
            .collect::<QueryResult<Vec<_>>>()?;
        let stmt = self
            .prepare_typed(sql, &bind_types)
            .await
            .map_err(ErrorHelper);
        Ok((stmt?, self))
    }
}

fn type_from_oid(t: &PgTypeMetadata) -> QueryResult<Type> {
    let oid = t
        .oid()
        .map_err(|e| diesel::result::Error::SerializationError(Box::new(e) as _))?;

    if let Some(tpe) = Type::from_oid(oid) {
        return Ok(tpe);
    }

    Ok(Type::new(
        "diesel_custom_type".into(),
        oid,
        tokio_postgres::types::Kind::Simple,
        "public".into(),
    ))
}

impl AsyncPgConnection {
    /// Build a transaction, specifying additional details such as isolation level
    ///
    /// See [`TransactionBuilder`] for more examples.
    ///
    /// [`TransactionBuilder`]: crate::pg::TransactionBuilder
    ///
    /// ```rust
    /// # include!("../doctest_setup.rs");
    /// # use scoped_futures::ScopedFutureExt;
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use schema::users::dsl::*;
    /// #     let conn = &mut connection_no_transaction().await;
    /// conn.build_transaction()
    ///     .read_only()
    ///     .serializable()
    ///     .deferrable()
    ///     .run(|conn| async move { Ok(()) }.scope_boxed())
    ///     .await
    /// # }
    /// ```
    pub fn build_transaction(&mut self) -> TransactionBuilder<Self> {
        TransactionBuilder::new(self)
    }

    /// Construct a new `AsyncPgConnection` instance from an existing [`tokio_postgres::Client`]
    pub async fn try_from(conn: tokio_postgres::Client) -> ConnectionResult<Self> {
        let mut conn = Self {
            conn: Arc::new(conn),
            stmt_cache: Arc::new(Mutex::new(StmtCache::new())),
            transaction_state: Arc::new(Mutex::new(AnsiTransactionManager::default())),
            metadata_cache: Arc::new(Mutex::new(Some(PgMetadataCache::new()))),
        };
        conn.set_config_options()
            .await
            .map_err(ConnectionError::CouldntSetupConfiguration)?;
        Ok(conn)
    }

    /// Constructs a cancellation token that can later be used to request cancellation of a query running on the connection associated with this client.
    pub fn cancel_token(&self) -> tokio_postgres::CancelToken {
        self.conn.cancel_token()
    }

    async fn set_config_options(&mut self) -> QueryResult<()> {
        use crate::run_query_dsl::RunQueryDsl;

        diesel::sql_query("SET TIME ZONE 'UTC'")
            .execute(self)
            .await?;
        diesel::sql_query("SET CLIENT_ENCODING TO 'UTF8'")
            .execute(self)
            .await?;
        Ok(())
    }

    async fn with_prepared_statement<'a, T, F, R>(
        raw_connection: Arc<tokio_postgres::Client>,
        stmt_cache: Arc<Mutex<StmtCache<diesel::pg::Pg, Statement>>>,
        metadata_cache: Arc<Mutex<Option<PgMetadataCache>>>,
        tm: Arc<Mutex<AnsiTransactionManager>>,
        query: T,
        callback: impl FnOnce(Arc<tokio_postgres::Client>, Statement, Vec<ToSqlHelper>) -> F,
    ) -> QueryResult<R>
    where
        T: QueryFragment<diesel::pg::Pg> + QueryId + Send,
        F: Future<Output = QueryResult<R>>,
    {
        let mut bind_collector;
        {
            loop {
                // we need a new bind collector per iteration here
                bind_collector = RawBytesBindCollector::<diesel::pg::Pg>::new();

                let (res, unresolved_types) = {
                    let mut metadata_cache_lock = metadata_cache.lock().await;
                    let mut metadata_lookup =
                        PgAsyncMetadataLookup::new(metadata_cache_lock.take().unwrap_or_default());

                    let res = query.collect_binds(
                        &mut bind_collector,
                        &mut metadata_lookup,
                        &diesel::pg::Pg,
                    );

                    let PgAsyncMetadataLookup {
                        unresolved_types,
                        metadata_cache,
                    } = metadata_lookup;
                    *metadata_cache_lock = Some(metadata_cache);
                    (res, unresolved_types)
                };

                if !unresolved_types.is_empty() {
                    for (schema, lookup_type_name) in unresolved_types {
                        // as this is an async call and we don't want to infect the whole diesel serialization
                        // api with async we just error out in the `PgMetadataLookup` implementation below if we encounter
                        // a type that is not cached yet
                        // If that's the case we will do the lookup here and try again as the
                        // type is now cached.
                        let type_metadata =
                            lookup_type(schema.clone(), lookup_type_name.clone(), &raw_connection)
                                .await?;
                        let mut metadata_cache_lock = metadata_cache.lock().await;
                        let metadata_cache =
                            if let Some(ref mut metadata_cache) = *metadata_cache_lock {
                                metadata_cache
                            } else {
                                *metadata_cache_lock = Some(Default::default());
                                metadata_cache_lock.as_mut().expect("We set it above")
                            };

                        metadata_cache.store_type(
                            PgMetadataCacheKey::new(
                                schema.map(Cow::Owned),
                                Cow::Owned(lookup_type_name),
                            ),
                            type_metadata,
                        );
                        // just try again to get the binds, now that we've inserted the
                        // type into the lookup list
                    }
                } else {
                    // bubble up any error as soon as we have done all lookups
                    res?;
                    break;
                }
            }
        }

        let stmt = {
            let mut stmt_cache = stmt_cache.lock().await;
            stmt_cache
                .cached_prepared_statement(
                    query,
                    &bind_collector.metadata,
                    raw_connection.clone(),
                    &diesel::pg::Pg,
                )
                .await?
                .0
                .clone()
        };

        let binds = bind_collector
            .metadata
            .into_iter()
            .zip(bind_collector.binds)
            .map(|(meta, bind)| ToSqlHelper(meta, bind))
            .collect::<Vec<_>>();
        let res = callback(raw_connection, stmt.clone(), binds).await;
        let mut tm = tm.lock().await;
        update_transaction_manager_status(res, &mut tm)
    }
}

struct PgAsyncMetadataLookup {
    unresolved_types: Vec<(Option<String>, String)>,
    metadata_cache: PgMetadataCache,
}

impl PgAsyncMetadataLookup {
    fn new(metadata_cache: PgMetadataCache) -> Self {
        Self {
            unresolved_types: Vec::new(),
            metadata_cache,
        }
    }
}

impl PgMetadataLookup for PgAsyncMetadataLookup {
    fn lookup_type(&mut self, type_name: &str, schema: Option<&str>) -> PgTypeMetadata {
        let cache_key =
            PgMetadataCacheKey::new(schema.map(Cow::Borrowed), Cow::Borrowed(type_name));

        if let Some(metadata) = self.metadata_cache.lookup_type(&cache_key) {
            metadata
        } else {
            let cache_key = cache_key.into_owned();
            self.unresolved_types
                .push((schema.map(ToOwned::to_owned), type_name.to_owned()));
            PgTypeMetadata::from_result(Err(FailedToLookupTypeError::new(cache_key)))
        }
    }
}

async fn lookup_type(
    schema: Option<String>,
    type_name: String,
    raw_connection: &tokio_postgres::Client,
) -> QueryResult<(u32, u32)> {
    let r = if let Some(schema) = schema.as_ref() {
        raw_connection
            .query_one(
                "SELECT pg_type.oid, pg_type.typarray FROM pg_type \
             INNER JOIN pg_namespace ON pg_type.typnamespace = pg_namespace.oid \
             WHERE pg_type.typname = $1 AND pg_namespace.nspname = $2 \
             LIMIT 1",
                &[&type_name, schema],
            )
            .await
            .map_err(ErrorHelper)?
    } else {
        raw_connection
            .query_one(
                "SELECT pg_type.oid, pg_type.typarray FROM pg_type \
             WHERE pg_type.oid = quote_ident($1)::regtype::oid \
             LIMIT 1",
                &[&type_name],
            )
            .await
            .map_err(ErrorHelper)?
    };
    Ok((r.get(0), r.get(1)))
}

#[cfg(any(feature = "deadpool", feature = "bb8", feature = "mobc"))]
impl crate::pooled_connection::PoolableConnection for AsyncPgConnection {}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::run_query_dsl::RunQueryDsl;
    use diesel::sql_types::Integer;
    use diesel::IntoSql;

    #[tokio::test]
    async fn pipelining() {
        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set in order to run tests");
        let mut conn = crate::AsyncPgConnection::establish(&database_url)
            .await
            .unwrap();

        let q1 = diesel::select(1_i32.into_sql::<Integer>());
        let q2 = diesel::select(2_i32.into_sql::<Integer>());

        let f1 = q1.get_result::<i32>(&mut conn);
        let f2 = q2.get_result::<i32>(&mut conn);

        let (r1, r2) = futures_util::try_join!(f1, f2).unwrap();

        assert_eq!(r1, 1);
        assert_eq!(r2, 2);
    }
}
