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
use diesel::connection::statement_cache::{PrepareForCache, StatementCacheKey};
use diesel::connection::Instrumentation;
use diesel::connection::InstrumentationEvent;
use diesel::connection::StrQueryHelper;
use diesel::pg::{
    Pg, PgMetadataCache, PgMetadataCacheKey, PgMetadataLookup, PgQueryBuilder, PgTypeMetadata,
};
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::query_builder::{AsQuery, QueryBuilder, QueryFragment, QueryId};
use diesel::result::{DatabaseErrorKind, Error};
use diesel::{ConnectionError, ConnectionResult, QueryResult};
use futures_util::future::BoxFuture;
use futures_util::future::Either;
use futures_util::stream::{BoxStream, TryStreamExt};
use futures_util::TryFutureExt;
use futures_util::{Future, FutureExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio_postgres::types::ToSql;
use tokio_postgres::types::Type;
use tokio_postgres::Statement;

pub use self::transaction_builder::TransactionBuilder;

mod error_helper;
mod row;
mod serialize;
mod transaction_builder;

const FAKE_OID: u32 = 0;

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
/// ## Pipelining
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
/// use diesel_async::RunQueryDsl;
///
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
/// ```
///
/// ## TLS
///
/// Connections created by [`AsyncPgConnection::establish`] do not support TLS.
///
/// TLS support for tokio_postgres connections is implemented by external crates, e.g. [tokio_postgres_rustls].
///
/// [`AsyncPgConnection::try_from_client_and_connection`] can be used to construct a connection from an existing
/// [`tokio_postgres::Connection`] with TLS enabled.
///
/// [tokio_postgres_rustls]: https://docs.rs/tokio-postgres-rustls/0.12.0/tokio_postgres_rustls/
pub struct AsyncPgConnection {
    conn: Arc<tokio_postgres::Client>,
    stmt_cache: Arc<Mutex<StmtCache<diesel::pg::Pg, Statement>>>,
    transaction_state: Arc<Mutex<AnsiTransactionManager>>,
    metadata_cache: Arc<Mutex<PgMetadataCache>>,
    connection_future: Option<broadcast::Receiver<Arc<tokio_postgres::Error>>>,
    shutdown_channel: Option<oneshot::Sender<()>>,
    // a sync mutex is fine here as we only hold it for a really short time
    instrumentation: Arc<std::sync::Mutex<Option<Box<dyn Instrumentation>>>>,
}

#[async_trait::async_trait]
impl SimpleAsyncConnection for AsyncPgConnection {
    async fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        self.record_instrumentation(InstrumentationEvent::start_query(&StrQueryHelper::new(
            query,
        )));
        let connection_future = self.connection_future.as_ref().map(|rx| rx.resubscribe());
        let batch_execute = self
            .conn
            .batch_execute(query)
            .map_err(ErrorHelper)
            .map_err(Into::into);
        let r = drive_future(connection_future, batch_execute).await;
        self.record_instrumentation(InstrumentationEvent::finish_query(
            &StrQueryHelper::new(query),
            r.as_ref().err(),
        ));
        r
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
        let mut instrumentation = diesel::connection::get_default_instrumentation();
        instrumentation.on_connection_event(InstrumentationEvent::start_establish_connection(
            database_url,
        ));
        let instrumentation = Arc::new(std::sync::Mutex::new(instrumentation));
        let (client, connection) = tokio_postgres::connect(database_url, tokio_postgres::NoTls)
            .await
            .map_err(ErrorHelper)?;

        let (error_rx, shutdown_tx) = drive_connection(connection);

        let r = Self::setup(
            client,
            Some(error_rx),
            Some(shutdown_tx),
            Arc::clone(&instrumentation),
        )
        .await;

        instrumentation
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .on_connection_event(InstrumentationEvent::finish_establish_connection(
                database_url,
                r.as_ref().err(),
            ));
        r
    }

    fn load<'conn, 'query, T>(&'conn mut self, source: T) -> Self::LoadFuture<'conn, 'query>
    where
        T: AsQuery + 'query,
        T::Query: QueryFragment<Self::Backend> + QueryId + 'query,
    {
        let query = source.as_query();
        let load_future = self.with_prepared_statement(query, load_prepared);

        self.run_with_connection_future(load_future)
    }

    fn execute_returning_count<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> Self::ExecuteFuture<'conn, 'query>
    where
        T: QueryFragment<Self::Backend> + QueryId + 'query,
    {
        let execute = self.with_prepared_statement(source, execute_prepared);
        self.run_with_connection_future(execute)
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

    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        // there should be no other pending future when this is called
        // that means there is only one instance of this arc and
        // we can simply access the inner data
        if let Some(instrumentation) = Arc::get_mut(&mut self.instrumentation) {
            instrumentation.get_mut().unwrap_or_else(|p| p.into_inner())
        } else {
            panic!("Cannot access shared instrumentation")
        }
    }

    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation) {
        self.instrumentation = Arc::new(std::sync::Mutex::new(Some(Box::new(instrumentation))));
    }
}

impl Drop for AsyncPgConnection {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_channel.take() {
            let _ = tx.send(());
        }
    }
}

async fn load_prepared(
    conn: Arc<tokio_postgres::Client>,
    stmt: Statement,
    binds: Vec<ToSqlHelper>,
) -> QueryResult<BoxStream<'static, QueryResult<PgRow>>> {
    let res = conn.query_raw(&stmt, binds).await.map_err(ErrorHelper)?;

    Ok(res
        .map_err(|e| diesel::result::Error::from(ErrorHelper(e)))
        .map_ok(PgRow::new)
        .boxed())
}

async fn execute_prepared(
    conn: Arc<tokio_postgres::Client>,
    stmt: Statement,
    binds: Vec<ToSqlHelper>,
) -> QueryResult<usize> {
    let binds = binds
        .iter()
        .map(|b| b as &(dyn ToSql + Sync))
        .collect::<Vec<_>>();

    let res = tokio_postgres::Client::execute(&conn, &stmt, &binds as &[_])
        .await
        .map_err(ErrorHelper)?;
    res.try_into()
        .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e)))
}

#[inline(always)]
fn update_transaction_manager_status<T>(
    query_result: QueryResult<T>,
    transaction_manager: &mut AnsiTransactionManager,
) -> QueryResult<T> {
    if let Err(diesel::result::Error::DatabaseError(DatabaseErrorKind::SerializationFailure, _)) =
        query_result
    {
        transaction_manager
            .status
            .set_requires_rollback_maybe_up_to_top_level(true)
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
        format!("diesel_custom_type_{oid}"),
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
        Self::setup(
            conn,
            None,
            None,
            Arc::new(std::sync::Mutex::new(
                diesel::connection::get_default_instrumentation(),
            )),
        )
        .await
    }

    /// Constructs a new `AsyncPgConnection` from an existing [`tokio_postgres::Client`] and
    /// [`tokio_postgres::Connection`]
    pub async fn try_from_client_and_connection<S>(
        client: tokio_postgres::Client,
        conn: tokio_postgres::Connection<tokio_postgres::Socket, S>,
    ) -> ConnectionResult<Self>
    where
        S: tokio_postgres::tls::TlsStream + Unpin + Send + 'static,
    {
        let (error_rx, shutdown_tx) = drive_connection(conn);

        Self::setup(
            client,
            Some(error_rx),
            Some(shutdown_tx),
            Arc::new(std::sync::Mutex::new(
                diesel::connection::get_default_instrumentation(),
            )),
        )
        .await
    }

    async fn setup(
        conn: tokio_postgres::Client,
        connection_future: Option<broadcast::Receiver<Arc<tokio_postgres::Error>>>,
        shutdown_channel: Option<oneshot::Sender<()>>,
        instrumentation: Arc<std::sync::Mutex<Option<Box<dyn Instrumentation>>>>,
    ) -> ConnectionResult<Self> {
        let mut conn = Self {
            conn: Arc::new(conn),
            stmt_cache: Arc::new(Mutex::new(StmtCache::new())),
            transaction_state: Arc::new(Mutex::new(AnsiTransactionManager::default())),
            metadata_cache: Arc::new(Mutex::new(PgMetadataCache::new())),
            connection_future,
            shutdown_channel,
            instrumentation,
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

        futures_util::try_join!(
            diesel::sql_query("SET TIME ZONE 'UTC'").execute(self),
            diesel::sql_query("SET CLIENT_ENCODING TO 'UTF8'").execute(self),
        )?;
        Ok(())
    }

    fn run_with_connection_future<'a, R: 'a>(
        &self,
        future: impl Future<Output = QueryResult<R>> + Send + 'a,
    ) -> BoxFuture<'a, QueryResult<R>> {
        let connection_future = self.connection_future.as_ref().map(|rx| rx.resubscribe());
        drive_future(connection_future, future).boxed()
    }

    fn with_prepared_statement<'a, T, F, R>(
        &mut self,
        query: T,
        callback: fn(Arc<tokio_postgres::Client>, Statement, Vec<ToSqlHelper>) -> F,
    ) -> BoxFuture<'a, QueryResult<R>>
    where
        T: QueryFragment<diesel::pg::Pg> + QueryId,
        F: Future<Output = QueryResult<R>> + Send + 'a,
        R: Send,
    {
        self.record_instrumentation(InstrumentationEvent::start_query(&diesel::debug_query(
            &query,
        )));
        // we explicilty descruct the query here before going into the async block
        //
        // That's required to remove the send bound from `T` as we have translated
        // the query type to just a string (for the SQL) and a bunch of bytes (for the binds)
        // which both are `Send`.
        // We also collect the query id (essentially an integer) and the safe_to_cache flag here
        // so there is no need to even access the query in the async block below
        let mut query_builder = PgQueryBuilder::default();

        let bind_data = construct_bind_data(&query);

        // The code that doesn't need the `T` generic parameter is in a separate function to reduce LLVM IR lines
        self.with_prepared_statement_after_sql_built(
            callback,
            query.is_safe_to_cache_prepared(&Pg),
            T::query_id(),
            query.to_sql(&mut query_builder, &Pg),
            query_builder,
            bind_data,
        )
    }

    fn with_prepared_statement_after_sql_built<'a, F, R>(
        &mut self,
        callback: fn(Arc<tokio_postgres::Client>, Statement, Vec<ToSqlHelper>) -> F,
        is_safe_to_cache_prepared: QueryResult<bool>,
        query_id: Option<std::any::TypeId>,
        to_sql_result: QueryResult<()>,
        query_builder: PgQueryBuilder,
        bind_data: BindData,
    ) -> BoxFuture<'a, QueryResult<R>>
    where
        F: Future<Output = QueryResult<R>> + Send + 'a,
        R: Send,
    {
        let raw_connection = self.conn.clone();
        let stmt_cache = self.stmt_cache.clone();
        let metadata_cache = self.metadata_cache.clone();
        let tm = self.transaction_state.clone();
        let instrumentation = self.instrumentation.clone();
        let BindData {
            collect_bind_result,
            fake_oid_locations,
            generated_oids,
            mut bind_collector,
        } = bind_data;

        async move {
            let sql = to_sql_result.map(|_| query_builder.finish())?;
            let res = async {
            let is_safe_to_cache_prepared = is_safe_to_cache_prepared?;
            collect_bind_result?;
            // Check whether we need to resolve some types at all
            //
            // If the user doesn't use custom types there is no need
            // to borther with that at all
            if let Some(ref unresolved_types) = generated_oids {
                let metadata_cache = &mut *metadata_cache.lock().await;
                let mut real_oids = HashMap::new();

                for ((schema, lookup_type_name), (fake_oid, fake_array_oid)) in
                    unresolved_types
                {
                    // for each unresolved item
                    // we check whether it's arleady in the cache
                    // or perform a lookup and insert it into the cache
                    let cache_key = PgMetadataCacheKey::new(
                        schema.as_deref().map(Into::into),
                        lookup_type_name.into(),
                    );
                    let real_metadata = if let Some(type_metadata) =
                        metadata_cache.lookup_type(&cache_key)
                    {
                        type_metadata
                    } else {
                        let type_metadata =
                            lookup_type(schema.clone(), lookup_type_name.clone(), &raw_connection)
                                .await?;
                        metadata_cache.store_type(cache_key, type_metadata);

                        PgTypeMetadata::from_result(Ok(type_metadata))
                    };
                    // let (fake_oid, fake_array_oid) = metadata_lookup.fake_oids(index);
                    let (real_oid, real_array_oid) = unwrap_oids(&real_metadata);
                    real_oids.extend([(*fake_oid, real_oid), (*fake_array_oid, real_array_oid)]);
                }

                // Replace fake OIDs with real OIDs in `bind_collector.metadata`
                for m in &mut bind_collector.metadata {
                    let (oid, array_oid) = unwrap_oids(m);
                    *m = PgTypeMetadata::new(
                        real_oids.get(&oid).copied().unwrap_or(oid),
                        real_oids.get(&array_oid).copied().unwrap_or(array_oid)
                    );
                }
                // Replace fake OIDs with real OIDs in `bind_collector.binds`
                for (bind_index, byte_index) in fake_oid_locations {
                    replace_fake_oid(&mut bind_collector.binds, &real_oids, bind_index, byte_index)
                        .ok_or_else(|| {
                            Error::SerializationError(
                                format!("diesel_async failed to replace a type OID serialized in bind value {bind_index}").into(),
                            )
                        })?;
                }
            }
            let key = match query_id {
                Some(id) => StatementCacheKey::Type(id),
                None => StatementCacheKey::Sql {
                    sql: sql.clone(),
                    bind_types: bind_collector.metadata.clone(),
                },
            };
            let stmt = {
                let mut stmt_cache = stmt_cache.lock().await;
                stmt_cache
                    .cached_prepared_statement(
                        key,
                        sql.clone(),
                        is_safe_to_cache_prepared,
                        &bind_collector.metadata,
                        raw_connection.clone(),
                        &instrumentation
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
                callback(raw_connection, stmt.clone(), binds).await
            };
            let res = res.await;
            let mut tm = tm.lock().await;
            let r = update_transaction_manager_status(res, &mut tm);
            instrumentation
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .on_connection_event(InstrumentationEvent::finish_query(
                    &StrQueryHelper::new(&sql),
                    r.as_ref().err(),
                ));

            r
        }
        .boxed()
    }

    fn record_instrumentation(&self, event: InstrumentationEvent<'_>) {
        self.instrumentation
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .on_connection_event(event);
    }
}

struct BindData {
    collect_bind_result: Result<(), Error>,
    fake_oid_locations: Vec<(usize, usize)>,
    generated_oids: GeneratedOidTypeMap,
    bind_collector: RawBytesBindCollector<Pg>,
}

fn construct_bind_data(query: &dyn QueryFragment<diesel::pg::Pg>) -> BindData {
    // we don't resolve custom types here yet, we do that later
    // in the async block below as we might need to perform lookup
    // queries for that.
    //
    // We apply this workaround to prevent requiring all the diesel
    // serialization code to beeing async
    //
    // We give out constant fake oids here to optimize for the "happy" path
    // without custom type lookup
    let mut bind_collector_0 = RawBytesBindCollector::<diesel::pg::Pg>::new();
    let mut metadata_lookup_0 = PgAsyncMetadataLookup {
        custom_oid: false,
        generated_oids: None,
        oid_generator: |_, _| (FAKE_OID, FAKE_OID),
    };
    let collect_bind_result_0 =
        query.collect_binds(&mut bind_collector_0, &mut metadata_lookup_0, &Pg);
    // we have encountered a custom type oid, so we need to perform more work here.
    // These oids can occure in two locations:
    //
    // * In the collected metadata -> relativly easy to resolve, just need to replace them below
    // * As part of the seralized bind blob -> hard to replace
    //
    // To address the second case, we perform a second run of the bind collector
    // with a different set of fake oids. Then we compare the output of the two runs
    // and use that information to infer where to replace bytes in the serialized output
    if metadata_lookup_0.custom_oid {
        // we try to get the maxium oid we encountered here
        // to be sure that we don't accidently give out a fake oid below that collides with
        // something
        let mut max_oid = bind_collector_0
            .metadata
            .iter()
            .flat_map(|t| {
                [
                    t.oid().unwrap_or_default(),
                    t.array_oid().unwrap_or_default(),
                ]
            })
            .max()
            .unwrap_or_default();
        let mut bind_collector_1 = RawBytesBindCollector::<diesel::pg::Pg>::new();
        let mut metadata_lookup_1 = PgAsyncMetadataLookup {
            custom_oid: false,
            generated_oids: Some(HashMap::new()),
            oid_generator: move |_, _| {
                max_oid += 2;
                (max_oid, max_oid + 1)
            },
        };
        let collect_bind_result_1 =
            query.collect_binds(&mut bind_collector_1, &mut metadata_lookup_1, &Pg);

        assert_eq!(
            bind_collector_0.binds.len(),
            bind_collector_0.metadata.len()
        );
        let fake_oid_locations = std::iter::zip(
            bind_collector_0
                .binds
                .iter()
                .zip(&bind_collector_0.metadata),
            &bind_collector_1.binds,
        )
        .enumerate()
        .flat_map(|(bind_index, ((bytes_0, metadata_0), bytes_1))| {
            // custom oids might appear in the serialized bind arguments for arrays or composite (record) types
            // in both cases the relevant buffer is a custom type on it's own
            // so we only need to check the cases that contain a fake OID on their own
            let (bytes_0, bytes_1) = if matches!(metadata_0.oid(), Ok(FAKE_OID)) {
                (
                    bytes_0.as_deref().unwrap_or_default(),
                    bytes_1.as_deref().unwrap_or_default(),
                )
            } else {
                // for all other cases, just return an empty
                // list to make the iteration below a no-op
                // and prevent the need of boxing
                (&[] as &[_], &[] as &[_])
            };
            let lookup_map = metadata_lookup_1
                .generated_oids
                .as_ref()
                .map(|map| {
                    map.values()
                        .flat_map(|(oid, array_oid)| [*oid, *array_oid])
                        .collect::<HashSet<_>>()
                })
                .unwrap_or_default();
            std::iter::zip(
                bytes_0.windows(std::mem::size_of_val(&FAKE_OID)),
                bytes_1.windows(std::mem::size_of_val(&FAKE_OID)),
            )
            .enumerate()
            .filter_map(move |(byte_index, (l, r))| {
                // here we infer if some byte sequence is a fake oid
                // We use the following conditions for that:
                //
                // * The first byte sequence matches the constant FAKE_OID
                // * The second sequence does not match the constant FAKE_OID
                // * The second sequence is contained in the set of generated oid,
                //   otherwise we get false positives around the boundary
                //   of a to be replaced byte sequence
                let r_val = u32::from_be_bytes(r.try_into().expect("That's the right size"));
                (l == FAKE_OID.to_be_bytes()
                    && r != FAKE_OID.to_be_bytes()
                    && lookup_map.contains(&r_val))
                .then_some((bind_index, byte_index))
            })
        })
        // Avoid storing the bind collectors in the returned Future
        .collect::<Vec<_>>();
        BindData {
            collect_bind_result: collect_bind_result_0.and(collect_bind_result_1),
            fake_oid_locations,
            generated_oids: metadata_lookup_1.generated_oids,
            bind_collector: bind_collector_1,
        }
    } else {
        BindData {
            collect_bind_result: collect_bind_result_0,
            fake_oid_locations: Vec::new(),
            generated_oids: None,
            bind_collector: bind_collector_0,
        }
    }
}

type GeneratedOidTypeMap = Option<HashMap<(Option<String>, String), (u32, u32)>>;

/// Collects types that need to be looked up, and causes fake OIDs to be written into the bind collector
/// so they can be replaced with asynchronously fetched OIDs after the original query is dropped
struct PgAsyncMetadataLookup<F: FnMut(&str, Option<&str>) -> (u32, u32) + 'static> {
    custom_oid: bool,
    generated_oids: GeneratedOidTypeMap,
    oid_generator: F,
}

impl<F> PgMetadataLookup for PgAsyncMetadataLookup<F>
where
    F: FnMut(&str, Option<&str>) -> (u32, u32) + 'static,
{
    fn lookup_type(&mut self, type_name: &str, schema: Option<&str>) -> PgTypeMetadata {
        self.custom_oid = true;

        let oid = if let Some(map) = &mut self.generated_oids {
            *map.entry((schema.map(ToOwned::to_owned), type_name.to_owned()))
                .or_insert_with(|| (self.oid_generator)(type_name, schema))
        } else {
            (self.oid_generator)(type_name, schema)
        };

        PgTypeMetadata::from_result(Ok(oid))
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

fn unwrap_oids(metadata: &PgTypeMetadata) -> (u32, u32) {
    let err_msg = "PgTypeMetadata is supposed to always be Ok here";
    (
        metadata.oid().expect(err_msg),
        metadata.array_oid().expect(err_msg),
    )
}

fn replace_fake_oid(
    binds: &mut [Option<Vec<u8>>],
    real_oids: &HashMap<u32, u32>,
    bind_index: usize,
    byte_index: usize,
) -> Option<()> {
    let serialized_oid = binds
        .get_mut(bind_index)?
        .as_mut()?
        .get_mut(byte_index..)?
        .first_chunk_mut::<4>()?;
    *serialized_oid = real_oids
        .get(&u32::from_be_bytes(*serialized_oid))?
        .to_be_bytes();
    Some(())
}

async fn drive_future<R>(
    connection_future: Option<broadcast::Receiver<Arc<tokio_postgres::Error>>>,
    client_future: impl Future<Output = Result<R, diesel::result::Error>>,
) -> Result<R, diesel::result::Error> {
    if let Some(mut connection_future) = connection_future {
        let client_future = std::pin::pin!(client_future);
        let connection_future = std::pin::pin!(connection_future.recv());
        match futures_util::future::select(client_future, connection_future).await {
            Either::Left((res, _)) => res,
            // we got an error from the background task
            // return it to the user
            Either::Right((Ok(e), _)) => Err(self::error_helper::from_tokio_postgres_error(e)),
            // seems like the background thread died for whatever reason
            Either::Right((Err(e), _)) => Err(diesel::result::Error::DatabaseError(
                DatabaseErrorKind::UnableToSendCommand,
                Box::new(e.to_string()),
            )),
        }
    } else {
        client_future.await
    }
}

fn drive_connection<S>(
    conn: tokio_postgres::Connection<tokio_postgres::Socket, S>,
) -> (
    broadcast::Receiver<Arc<tokio_postgres::Error>>,
    oneshot::Sender<()>,
)
where
    S: tokio_postgres::tls::TlsStream + Unpin + Send + 'static,
{
    let (error_tx, error_rx) = tokio::sync::broadcast::channel(1);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        match futures_util::future::select(shutdown_rx, conn).await {
            Either::Left(_) | Either::Right((Ok(_), _)) => {}
            Either::Right((Err(e), _)) => {
                let _ = error_tx.send(Arc::new(e));
            }
        }
    });

    (error_rx, shutdown_tx)
}

#[cfg(any(
    feature = "deadpool",
    feature = "bb8",
    feature = "mobc",
    feature = "r2d2"
))]
impl crate::pooled_connection::PoolableConnection for AsyncPgConnection {
    fn is_broken(&mut self) -> bool {
        use crate::TransactionManager;

        Self::TransactionManager::is_broken_transaction_manager(self) || self.conn.is_closed()
    }
}

#[cfg(test)]
mod tests {
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
