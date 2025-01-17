//! This module contains a wrapper type
//! that provides a [`crate::AsyncConnection`]
//! implementation for types that implement
//! [`diesel::Connection`]. Using this type
//! might be useful for the following usecases:
//!
//! * using a sync Connection implementation in async context
//! * using the same code base for async crates needing multiple backends

use crate::{AsyncConnection, SimpleAsyncConnection, TransactionManager};
use diesel::backend::{Backend, DieselReserveSpecialization};
use diesel::connection::{CacheSize, Instrumentation};
use diesel::connection::{
    Connection, LoadConnection, TransactionManagerStatus, WithMetadataLookup,
};
use diesel::query_builder::{
    AsQuery, CollectedQuery, MoveableBindCollector, QueryBuilder, QueryFragment, QueryId,
};
use diesel::row::IntoOwnedRow;
use diesel::{ConnectionResult, QueryResult};
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use futures_util::{FutureExt, StreamExt, TryFutureExt};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use tokio::task::JoinError;

#[cfg(feature = "sqlite")]
mod sqlite;

fn from_tokio_join_error(join_error: JoinError) -> diesel::result::Error {
    diesel::result::Error::DatabaseError(
        diesel::result::DatabaseErrorKind::UnableToSendCommand,
        Box::new(join_error.to_string()),
    )
}

/// A wrapper of a [`diesel::connection::Connection`] usable in async context.
///
/// It implements AsyncConnection if [`diesel::connection::Connection`] fullfils requirements:
/// * it's a [`diesel::connection::LoadConnection`]
/// * its [`diesel::connection::Connection::Backend`] has a [`diesel::query_builder::BindCollector`] implementing [`diesel::query_builder::MoveableBindCollector`]
/// * its [`diesel::connection::LoadConnection::Row`] implements [`diesel::row::IntoOwnedRow`]
///
/// Internally this wrapper type will use `spawn_blocking` on tokio
/// to execute the request on the inner connection. This implies a
/// dependency on tokio and that the runtime is running.
///
/// Note that only SQLite is supported at the moment.
///
/// # Examples
///
/// ```rust
/// # include!("../doctest_setup.rs");
/// use diesel_async::RunQueryDsl;
/// use schema::users;
///
/// async fn some_async_fn() {
/// # let database_url = database_url();
///          use diesel_async::AsyncConnection;
///          use diesel::sqlite::SqliteConnection;
///          let mut conn =
///          SyncConnectionWrapper::<SqliteConnection>::establish(&database_url).await.unwrap();
/// # create_tables(&mut conn).await;
///
///          let all_users = users::table.load::<(i32, String)>(&mut conn).await.unwrap();
/// #         assert_eq!(all_users.len(), 2);
/// }
///
/// # #[cfg(feature = "sqlite")]
/// # #[tokio::main]
/// # async fn main() {
/// #    some_async_fn().await;
/// # }
/// ```
pub struct SyncConnectionWrapper<C> {
    inner: Arc<Mutex<C>>,
}

impl<C> SimpleAsyncConnection for SyncConnectionWrapper<C>
where
    C: diesel::connection::Connection + 'static,
{
    async fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        let query = query.to_string();
        self.spawn_blocking(move |inner| inner.batch_execute(query.as_str()))
            .await
    }
}

impl<C, MD, O> AsyncConnection for SyncConnectionWrapper<C>
where
    // Backend bounds
    <C as Connection>::Backend: std::default::Default + DieselReserveSpecialization,
    <C::Backend as Backend>::QueryBuilder: std::default::Default,
    // Connection bounds
    C: Connection + LoadConnection + WithMetadataLookup + 'static,
    <C as Connection>::TransactionManager: Send,
    // BindCollector bounds
    MD: Send + 'static,
    for<'a> <C::Backend as Backend>::BindCollector<'a>:
        MoveableBindCollector<C::Backend, BindData = MD> + std::default::Default,
    // Row bounds
    O: 'static + Send + for<'conn> diesel::row::Row<'conn, C::Backend>,
    for<'conn, 'query> <C as LoadConnection>::Row<'conn, 'query>:
        IntoOwnedRow<'conn, <C as Connection>::Backend, OwnedRow = O>,
{
    type LoadFuture<'conn, 'query> = BoxFuture<'query, QueryResult<Self::Stream<'conn, 'query>>>;
    type ExecuteFuture<'conn, 'query> = BoxFuture<'query, QueryResult<usize>>;
    type Stream<'conn, 'query> = BoxStream<'static, QueryResult<Self::Row<'conn, 'query>>>;
    type Row<'conn, 'query> = O;
    type Backend = <C as Connection>::Backend;
    type TransactionManager = SyncTransactionManagerWrapper<<C as Connection>::TransactionManager>;

    async fn establish(database_url: &str) -> ConnectionResult<Self> {
        let database_url = database_url.to_string();
        tokio::task::spawn_blocking(move || C::establish(&database_url))
            .await
            .unwrap_or_else(|e| Err(diesel::ConnectionError::BadConnection(e.to_string())))
            .map(|c| SyncConnectionWrapper::new(c))
    }

    fn load<'conn, 'query, T>(&'conn mut self, source: T) -> Self::LoadFuture<'conn, 'query>
    where
        T: AsQuery + 'query,
        T::Query: QueryFragment<Self::Backend> + QueryId + 'query,
    {
        self.execute_with_prepared_query(source.as_query(), |conn, query| {
            use diesel::row::IntoOwnedRow;
            let mut cache = <<<C as LoadConnection>::Row<'_, '_> as IntoOwnedRow<
                <C as Connection>::Backend,
            >>::Cache as Default>::default();
            let cursor = conn.load(&query)?;

            let size_hint = cursor.size_hint();
            let mut out = Vec::with_capacity(size_hint.1.unwrap_or(size_hint.0));
            // we use an explicit loop here to easily propagate possible errors
            // as early as possible
            for row in cursor {
                out.push(Ok(IntoOwnedRow::into_owned(row?, &mut cache)));
            }

            Ok(out)
        })
        .map_ok(|rows| futures_util::stream::iter(rows).boxed())
        .boxed()
    }

    fn execute_returning_count<'query, T>(&mut self, source: T) -> Self::ExecuteFuture<'_, 'query>
    where
        T: QueryFragment<Self::Backend> + QueryId,
    {
        self.execute_with_prepared_query(source, |conn, query| conn.execute_returning_count(&query))
    }

    fn transaction_state(
        &mut self,
    ) -> &mut <Self::TransactionManager as TransactionManager<Self>>::TransactionStateData {
        self.exclusive_connection().transaction_state()
    }

    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        // there should be no other pending future when this is called
        // that means there is only one instance of this arc and
        // we can simply access the inner data
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner
                .get_mut()
                .unwrap_or_else(|p| p.into_inner())
                .instrumentation()
        } else {
            panic!("Cannot access shared instrumentation")
        }
    }

    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation) {
        // there should be no other pending future when this is called
        // that means there is only one instance of this arc and
        // we can simply access the inner data
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner
                .get_mut()
                .unwrap_or_else(|p| p.into_inner())
                .set_instrumentation(instrumentation)
        } else {
            panic!("Cannot access shared instrumentation")
        }
    }

    fn set_prepared_statement_cache_size(&mut self, size: CacheSize) {
        // there should be no other pending future when this is called
        // that means there is only one instance of this arc and
        // we can simply access the inner data
        if let Some(inner) = Arc::get_mut(&mut self.inner) {
            inner
                .get_mut()
                .unwrap_or_else(|p| p.into_inner())
                .set_prepared_statement_cache_size(size)
        } else {
            panic!("Cannot access shared cache")
        }
    }
}

/// A wrapper of a diesel transaction manager usable in async context.
pub struct SyncTransactionManagerWrapper<T>(PhantomData<T>);

impl<T, C> TransactionManager<SyncConnectionWrapper<C>> for SyncTransactionManagerWrapper<T>
where
    SyncConnectionWrapper<C>: AsyncConnection,
    C: Connection + 'static,
    T: diesel::connection::TransactionManager<C> + Send,
{
    type TransactionStateData = T::TransactionStateData;

    async fn begin_transaction(conn: &mut SyncConnectionWrapper<C>) -> QueryResult<()> {
        conn.spawn_blocking(move |inner| T::begin_transaction(inner))
            .await
    }

    async fn commit_transaction(conn: &mut SyncConnectionWrapper<C>) -> QueryResult<()> {
        conn.spawn_blocking(move |inner| T::commit_transaction(inner))
            .await
    }

    async fn rollback_transaction(conn: &mut SyncConnectionWrapper<C>) -> QueryResult<()> {
        conn.spawn_blocking(move |inner| T::rollback_transaction(inner))
            .await
    }

    fn transaction_manager_status_mut(
        conn: &mut SyncConnectionWrapper<C>,
    ) -> &mut TransactionManagerStatus {
        T::transaction_manager_status_mut(conn.exclusive_connection())
    }
}

impl<C> SyncConnectionWrapper<C> {
    /// Builds a wrapper with this underlying sync connection
    pub fn new(connection: C) -> Self
    where
        C: Connection,
    {
        SyncConnectionWrapper {
            inner: Arc::new(Mutex::new(connection)),
        }
    }

    /// Run a operation directly with the inner connection
    ///
    /// This function is usful to register custom functions
    /// and collection for Sqlite for example
    ///
    /// # Example
    ///
    /// ```rust
    /// # include!("../doctest_setup.rs");
    /// # #[tokio::main]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     let mut conn = establish_connection().await;
    /// conn.spawn_blocking(|conn| {
    ///    // sqlite.rs sqlite NOCASE only works for ASCII characters,
    ///    // this collation allows handling UTF-8 (barring locale differences)
    ///    conn.register_collation("RUSTNOCASE", |rhs, lhs| {
    ///     rhs.to_lowercase().cmp(&lhs.to_lowercase())
    ///   })
    /// }).await
    ///
    /// # }
    /// ```
    pub fn spawn_blocking<'a, R>(
        &mut self,
        task: impl FnOnce(&mut C) -> QueryResult<R> + Send + 'static,
    ) -> BoxFuture<'a, QueryResult<R>>
    where
        C: Connection + 'static,
        R: Send + 'static,
    {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut inner = inner.lock().unwrap_or_else(|poison| {
                // try to be resilient by providing the guard
                inner.clear_poison();
                poison.into_inner()
            });
            task(&mut inner)
        })
        .unwrap_or_else(|err| QueryResult::Err(from_tokio_join_error(err)))
        .boxed()
    }

    fn execute_with_prepared_query<'a, MD, Q, R>(
        &mut self,
        query: Q,
        callback: impl FnOnce(&mut C, &CollectedQuery<MD>) -> QueryResult<R> + Send + 'static,
    ) -> BoxFuture<'a, QueryResult<R>>
    where
        // Backend bounds
        <C as Connection>::Backend: std::default::Default + DieselReserveSpecialization,
        <C::Backend as Backend>::QueryBuilder: std::default::Default,
        // Connection bounds
        C: Connection + LoadConnection + WithMetadataLookup + 'static,
        <C as Connection>::TransactionManager: Send,
        // BindCollector bounds
        MD: Send + 'static,
        for<'b> <C::Backend as Backend>::BindCollector<'b>:
            MoveableBindCollector<C::Backend, BindData = MD> + std::default::Default,
        // Arguments/Return bounds
        Q: QueryFragment<C::Backend> + QueryId,
        R: Send + 'static,
    {
        let backend = C::Backend::default();

        let (collect_bind_result, collector_data) = {
            let exclusive = self.inner.clone();
            let mut inner = exclusive.lock().unwrap_or_else(|poison| {
                // try to be resilient by providing the guard
                exclusive.clear_poison();
                poison.into_inner()
            });
            let mut bind_collector =
                <<C::Backend as Backend>::BindCollector<'_> as Default>::default();
            let metadata_lookup = inner.metadata_lookup();
            let result = query.collect_binds(&mut bind_collector, metadata_lookup, &backend);
            let collector_data = bind_collector.moveable();

            (result, collector_data)
        };

        let mut query_builder = <<C::Backend as Backend>::QueryBuilder as Default>::default();
        let sql = query
            .to_sql(&mut query_builder, &backend)
            .map(|_| query_builder.finish());
        let is_safe_to_cache_prepared = query.is_safe_to_cache_prepared(&backend);

        self.spawn_blocking(|inner| {
            collect_bind_result?;
            let query = CollectedQuery::new(sql?, is_safe_to_cache_prepared?, collector_data);
            callback(inner, &query)
        })
    }

    /// Gets an exclusive access to the underlying diesel Connection
    ///
    /// It panics in case of shared access.
    /// This is typically used only used during transaction.
    pub(self) fn exclusive_connection(&mut self) -> &mut C
    where
        C: Connection,
    {
        // there should be no other pending future when this is called
        // that means there is only one instance of this Arc and
        // we can simply access the inner data
        if let Some(conn_mutex) = Arc::get_mut(&mut self.inner) {
            conn_mutex
                .get_mut()
                .expect("Mutex is poisoned, a thread must have panicked holding it.")
        } else {
            panic!("Cannot access shared transaction state")
        }
    }
}

#[cfg(any(
    feature = "deadpool",
    feature = "bb8",
    feature = "mobc",
    feature = "r2d2"
))]
impl<C> crate::pooled_connection::PoolableConnection for SyncConnectionWrapper<C>
where
    Self: AsyncConnection,
{
    fn is_broken(&mut self) -> bool {
        Self::TransactionManager::is_broken_transaction_manager(self)
    }
}
