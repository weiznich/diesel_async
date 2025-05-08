//! This module contains support using diesel-async with
//! various async rust connection pooling solutions
//!
//! See the concrete pool implementations for examples:
//! * [deadpool](self::deadpool)
//! * [bb8](self::bb8)
//! * [mobc](self::mobc)
use crate::{AsyncConnection, SimpleAsyncConnection};
use crate::{TransactionManager, UpdateAndFetchResults};
use diesel::associations::HasTable;
use diesel::connection::{CacheSize, Instrumentation};
use diesel::QueryResult;
use futures_util::future::BoxFuture;
use futures_util::{future, FutureExt};
use std::borrow::Cow;
use std::fmt;
use std::future::Future;
use std::ops::DerefMut;

#[cfg(feature = "bb8")]
pub mod bb8;
#[cfg(feature = "deadpool")]
pub mod deadpool;
#[cfg(feature = "mobc")]
pub mod mobc;

/// The error used when managing connections with `deadpool`.
#[derive(Debug)]
pub enum PoolError {
    /// An error occurred establishing the connection
    ConnectionError(diesel::result::ConnectionError),

    /// An error occurred pinging the database
    QueryError(diesel::result::Error),
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            PoolError::ConnectionError(ref e) => e.fmt(f),
            PoolError::QueryError(ref e) => e.fmt(f),
        }
    }
}

impl std::error::Error for PoolError {}

/// Type of the custom setup closure passed to [`ManagerConfig::custom_setup`]
pub type SetupCallback<C> =
    Box<dyn Fn(&str) -> future::BoxFuture<diesel::ConnectionResult<C>> + Send + Sync>;

/// Type of the recycle check callback for the [`RecyclingMethod::CustomFunction`] variant
pub type RecycleCheckCallback<C> =
    dyn Fn(&mut C) -> future::BoxFuture<QueryResult<()>> + Send + Sync;

/// Possible methods of how a connection is recycled.
#[derive(Default)]
pub enum RecyclingMethod<C> {
    /// Only check for open transactions when recycling existing connections
    /// Unless you have special needs this is a safe choice.
    ///
    /// If the database connection is closed you will recieve an error on the first place
    /// you actually try to use the connection
    Fast,
    /// In addition to checking for open transactions a test query is executed
    ///
    /// This is slower, but guarantees that the database connection is ready to be used.
    #[default]
    Verified,
    /// Like `Verified` but with a custom query
    CustomQuery(Cow<'static, str>),
    /// Like `Verified` but with a custom callback that allows to perform more checks
    ///
    /// The connection is only recycled if the callback returns `Ok(())`
    CustomFunction(Box<RecycleCheckCallback<C>>),
}

impl<C: fmt::Debug> fmt::Debug for RecyclingMethod<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fast => write!(f, "Fast"),
            Self::Verified => write!(f, "Verified"),
            Self::CustomQuery(arg0) => f.debug_tuple("CustomQuery").field(arg0).finish(),
            Self::CustomFunction(_) => f.debug_tuple("CustomFunction").finish(),
        }
    }
}

/// Configuration object for a Manager.
///
/// This makes it possible to specify which [`RecyclingMethod`]
/// should be used when retrieving existing objects from the `Pool`
/// and it allows to provide a custom setup function.
#[non_exhaustive]
pub struct ManagerConfig<C> {
    /// Method of how a connection is recycled. See [RecyclingMethod].
    pub recycling_method: RecyclingMethod<C>,
    /// Construct a new connection manger
    /// with a custom setup procedure
    ///
    /// This can be used to for example establish a SSL secured
    /// postgres connection
    pub custom_setup: SetupCallback<C>,
}

impl<C> Default for ManagerConfig<C>
where
    C: AsyncConnection + 'static,
{
    fn default() -> Self {
        Self {
            recycling_method: Default::default(),
            custom_setup: Box::new(|url| C::establish(url).boxed()),
        }
    }
}

/// An connection manager for use with diesel-async.
///
/// See the concrete pool implementations for examples:
/// * [deadpool](self::deadpool)
/// * [bb8](self::bb8)
/// * [mobc](self::mobc)
#[allow(dead_code)]
pub struct AsyncDieselConnectionManager<C> {
    connection_url: String,
    manager_config: ManagerConfig<C>,
}

impl<C> fmt::Debug for AsyncDieselConnectionManager<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AsyncDieselConnectionManager<{}>",
            std::any::type_name::<C>()
        )
    }
}

impl<C> AsyncDieselConnectionManager<C>
where
    C: AsyncConnection + 'static,
{
    /// Returns a new connection manager,
    /// which establishes connections to the given database URL.
    #[must_use]
    pub fn new(connection_url: impl Into<String>) -> Self
    where
        C: AsyncConnection + 'static,
    {
        Self::new_with_config(connection_url, Default::default())
    }

    /// Returns a new connection manager,
    /// which establishes connections with the given database URL
    /// and that uses the specified configuration
    #[must_use]
    pub fn new_with_config(
        connection_url: impl Into<String>,
        manager_config: ManagerConfig<C>,
    ) -> Self {
        Self {
            connection_url: connection_url.into(),
            manager_config,
        }
    }
}

impl<C> SimpleAsyncConnection for C
where
    C: DerefMut + Send,
    C::Target: SimpleAsyncConnection + Send,
{
    async fn batch_execute(&mut self, query: &str) -> diesel::QueryResult<()> {
        let conn = self.deref_mut();
        conn.batch_execute(query).await
    }
}

impl<C> AsyncConnection for C
where
    C: DerefMut + Send,
    C::Target: AsyncConnection,
{
    type ExecuteFuture<'conn, 'query> =
        <C::Target as AsyncConnection>::ExecuteFuture<'conn, 'query>;
    type LoadFuture<'conn, 'query> = <C::Target as AsyncConnection>::LoadFuture<'conn, 'query>;
    type Stream<'conn, 'query> = <C::Target as AsyncConnection>::Stream<'conn, 'query>;
    type Row<'conn, 'query> = <C::Target as AsyncConnection>::Row<'conn, 'query>;

    type Backend = <C::Target as AsyncConnection>::Backend;

    type TransactionManager =
        PoolTransactionManager<<C::Target as AsyncConnection>::TransactionManager>;

    #[cfg(not(target_arch = "wasm32"))]
    async fn establish(_database_url: &str) -> diesel::ConnectionResult<Self> {
        Err(diesel::result::ConnectionError::BadConnection(
            String::from("Cannot directly establish a pooled connection"),
        ))
    }

    fn load<'conn, 'query, T>(&'conn mut self, source: T) -> Self::LoadFuture<'conn, 'query>
    where
        T: diesel::query_builder::AsQuery + 'query,
        T::Query: diesel::query_builder::QueryFragment<Self::Backend>
            + diesel::query_builder::QueryId
            + 'query,
    {
        let conn = self.deref_mut();
        conn.load(source)
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
        let conn = self.deref_mut();
        conn.execute_returning_count(source)
    }

    fn transaction_state(
        &mut self,
    ) -> &mut <Self::TransactionManager as crate::transaction_manager::TransactionManager<Self>>::TransactionStateData{
        let conn = self.deref_mut();
        conn.transaction_state()
    }

    async fn begin_test_transaction(&mut self) -> diesel::QueryResult<()> {
        self.deref_mut().begin_test_transaction().await
    }

    fn instrumentation(&mut self) -> &mut dyn Instrumentation {
        self.deref_mut().instrumentation()
    }

    fn set_instrumentation(&mut self, instrumentation: impl Instrumentation) {
        self.deref_mut().set_instrumentation(instrumentation);
    }

    fn set_prepared_statement_cache_size(&mut self, size: CacheSize) {
        self.deref_mut().set_prepared_statement_cache_size(size);
    }
}

#[doc(hidden)]
#[allow(missing_debug_implementations)]
pub struct PoolTransactionManager<TM>(std::marker::PhantomData<TM>);

impl<C, TM> TransactionManager<C> for PoolTransactionManager<TM>
where
    C: DerefMut + Send,
    C::Target: AsyncConnection<TransactionManager = TM>,
    TM: TransactionManager<C::Target>,
{
    type TransactionStateData = TM::TransactionStateData;

    async fn begin_transaction(conn: &mut C) -> diesel::QueryResult<()> {
        TM::begin_transaction(&mut **conn).await
    }

    async fn rollback_transaction(conn: &mut C) -> diesel::QueryResult<()> {
        TM::rollback_transaction(&mut **conn).await
    }

    async fn commit_transaction(conn: &mut C) -> diesel::QueryResult<()> {
        TM::commit_transaction(&mut **conn).await
    }

    fn transaction_manager_status_mut(
        conn: &mut C,
    ) -> &mut diesel::connection::TransactionManagerStatus {
        TM::transaction_manager_status_mut(&mut **conn)
    }

    fn is_broken_transaction_manager(conn: &mut C) -> bool {
        TM::is_broken_transaction_manager(&mut **conn)
    }
}

impl<Changes, Output, Conn> UpdateAndFetchResults<Changes, Output> for Conn
where
    Conn: DerefMut + Send,
    Changes: diesel::prelude::Identifiable + HasTable + Send,
    Conn::Target: UpdateAndFetchResults<Changes, Output>,
{
    fn update_and_fetch<'conn, 'changes>(
        &'conn mut self,
        changeset: Changes,
    ) -> BoxFuture<'changes, QueryResult<Output>>
    where
        Changes: 'changes,
        'conn: 'changes,
        Self: 'changes,
    {
        self.deref_mut().update_and_fetch(changeset)
    }
}

#[derive(diesel::query_builder::QueryId)]
struct CheckConnectionQuery;

impl<DB> diesel::query_builder::QueryFragment<DB> for CheckConnectionQuery
where
    DB: diesel::backend::Backend,
{
    fn walk_ast<'b>(
        &'b self,
        mut pass: diesel::query_builder::AstPass<'_, 'b, DB>,
    ) -> diesel::QueryResult<()> {
        pass.push_sql("SELECT 1");
        Ok(())
    }
}

impl diesel::query_builder::Query for CheckConnectionQuery {
    type SqlType = diesel::sql_types::Integer;
}

impl<C> diesel::query_dsl::RunQueryDsl<C> for CheckConnectionQuery {}

#[doc(hidden)]
pub trait PoolableConnection: AsyncConnection {
    /// Check if a connection is still valid
    ///
    /// The default implementation will perform a check based on the provided
    /// recycling method variant
    fn ping(
        &mut self,
        config: &RecyclingMethod<Self>,
    ) -> impl Future<Output = diesel::QueryResult<()>> + Send
    where
        for<'a> Self: 'a,
        diesel::dsl::select<diesel::dsl::AsExprOf<i32, diesel::sql_types::Integer>>:
            crate::methods::ExecuteDsl<Self>,
        diesel::query_builder::SqlQuery: crate::methods::ExecuteDsl<Self>,
    {
        use crate::run_query_dsl::RunQueryDsl;
        use diesel::IntoSql;

        async move {
            match config {
                RecyclingMethod::Fast => Ok(()),
                RecyclingMethod::Verified => {
                    diesel::select(1_i32.into_sql::<diesel::sql_types::Integer>())
                        .execute(self)
                        .await
                        .map(|_| ())
                }
                RecyclingMethod::CustomQuery(query) => diesel::sql_query(query.as_ref())
                    .execute(self)
                    .await
                    .map(|_| ()),
                RecyclingMethod::CustomFunction(c) => c(self).await,
            }
        }
    }

    /// Checks if the connection is broken and should not be reused
    ///
    /// This method should return only contain a fast non-blocking check
    /// if the connection is considered to be broken or not. See
    /// [ManageConnection::has_broken] for details.
    ///
    /// The default implementation uses
    /// [TransactionManager::is_broken_transaction_manager].
    fn is_broken(&mut self) -> bool {
        Self::TransactionManager::is_broken_transaction_manager(self)
    }
}
