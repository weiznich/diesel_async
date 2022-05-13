use core::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;
use deadpool::managed::Manager;
use diesel::{
    query_builder::{AsQuery, QueryFragment, QueryId},
    ConnectionError, ConnectionResult, QueryResult,
};

use crate::{
    AsyncConnection, AsyncConnectionGatWorkaround, SimpleAsyncConnection, TransactionManager, RunQueryDsl,
};

pub use deadpool::managed::reexports::*;

pub type PooledConnection<CM> = deadpool::managed::Object<CM>;

pub type Pool<C> = deadpool::managed::Pool<ConnectionManager<C>>;

pub type RecycleResult = deadpool::managed::RecycleResult<Error>;

pub type RecycleError = deadpool::managed::RecycleError<Error>;

// TODO: Do we need re-export other items from deadpool::managed_reexports! ?
// Note we are not using deadpool::managed_reexports! as it doesn't support generic ConnectionManager

/// A deadpool connection manager for use with async Diesel.
///
/// See the [deadpool documentation] for usage examples.
///
/// [deadpool documentation]: deadpool
pub struct ConnectionManager<C> {
    // TODO: Ideally there should be common statement cache.
    database_url: String,
    _marker: PhantomData<C>,
}

unsafe impl<C: Send> Sync for ConnectionManager<C> {}

impl<C: AsyncConnection> fmt::Debug for ConnectionManager<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConnectionManager<{}>", std::any::type_name::<C>())
    }
}

impl<C> ConnectionManager<C> {
    /// Returns a new connection manager,
    /// which establishes connections to the given database URL.
    pub fn new<S: Into<String>>(database_url: S) -> Self {
        ConnectionManager {
            database_url: database_url.into(),
            _marker: PhantomData,
        }
    }
}

/// A trait indicating a connection could be used inside a pool
#[async_trait]
pub trait ManagedAsyncConnection: AsyncConnection {
    /// Check if a connection is still valid
    async fn recycle(&mut self) -> QueryResult<()>;
}

#[cfg(feature = "postgres")]
#[async_trait]
impl ManagedAsyncConnection for crate::pg::AsyncPgConnection {
    async fn recycle(&mut self) -> QueryResult<()> {
        // TODO: ping with query can be expensive for some use cases
        diesel::sql_query("SELECT 1").execute(self).await.map(|_| ())
    }
}

#[cfg(feature = "mysql")]
#[async_trait]
impl ManagedAsyncConnection for crate::mysql::AsyncMysqlConnection {
    async fn recycle(&mut self) -> QueryResult<()> {
        // TODO: ping with query can be expensive for some use cases
        diesel::sql_query("SELECT 1").execute(self).await.map(|_| ())
    }
}

#[async_trait]
impl<C: ManagedAsyncConnection> Manager for ConnectionManager<C> {
    type Type = C;
    type Error = Error;

    async fn create(&self) -> Result<C, Self::Error> {
        C::establish(&self.database_url)
            .await
            .map_err(Error::ConnectionError)
    }

    async fn recycle(&self, conn: &mut C) -> RecycleResult {
        conn.recycle()
            .await
            .map_err(|e| RecycleError::Backend(Error::QueryError(e)))
    }

    fn detach(&self, _obj: &mut Self::Type) {
        // TODO: Can be used to detach from common cache
    }
}

#[async_trait::async_trait]
impl<CM> SimpleAsyncConnection for PooledConnection<CM>
where
    CM: Manager,
    CM::Type: AsyncConnection,
{
    async fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        (&mut **self).batch_execute(query).await
    }
}

impl<'a, CM> AsyncConnectionGatWorkaround<'a, <CM::Type as AsyncConnection>::Backend>
    for PooledConnection<CM>
where
    CM: Manager,
    CM::Type: AsyncConnection,
{
    type Stream = <CM::Type as AsyncConnectionGatWorkaround<
        'a,
        <CM::Type as AsyncConnection>::Backend,
    >>::Stream;

    type Row =
        <CM::Type as AsyncConnectionGatWorkaround<'a, <CM::Type as AsyncConnection>::Backend>>::Row;
}

#[async_trait::async_trait]
impl<CM> AsyncConnection for PooledConnection<CM>
where
    CM: Manager,
    CM::Type: AsyncConnection,
{
    type Backend = <CM::Type as AsyncConnection>::Backend;

    type TransactionManager =
        PoolTransactionManager<<CM::Type as AsyncConnection>::TransactionManager>;

    async fn establish(_: &str) -> ConnectionResult<Self> {
        Err(ConnectionError::BadConnection(String::from(
            "Cannot directly establish a pooled connection",
        )))
    }

    async fn load<'a, S>(
        &'a mut self,
        source: S,
    ) -> QueryResult<<Self as AsyncConnectionGatWorkaround<'a, Self::Backend>>::Stream>
    where
        S: AsQuery + Send,
        S::Query: QueryFragment<Self::Backend> + QueryId + Send,
    {
        todo!()
    }

    async fn execute_returning_count<S>(&mut self, source: S) -> QueryResult<usize>
    where
        S: QueryFragment<Self::Backend> + QueryId + Send,
    {
        (&mut **self).execute_returning_count(source).await
    }

    async fn begin_test_transaction(&mut self) -> QueryResult<()> {
        (&mut **self).begin_test_transaction().await
    }

    fn transaction_state(
        &mut self,
    ) -> &mut <Self::TransactionManager as TransactionManager<Self>>::TransactionStateData {
        (&mut **self).transaction_state()
    }
}

#[doc(hidden)]
#[allow(missing_debug_implementations)]
pub struct PoolTransactionManager<TM>(std::marker::PhantomData<TM>);

#[async_trait::async_trait]
impl<CM, TM> TransactionManager<PooledConnection<CM>> for PoolTransactionManager<TM>
where
    CM: Manager,
    CM::Type: AsyncConnection<TransactionManager = TM>,
    TM: TransactionManager<CM::Type>,
{
    type TransactionStateData = TM::TransactionStateData;

    async fn begin_transaction(conn: &mut PooledConnection<CM>) -> QueryResult<()> {
        TM::begin_transaction(&mut **conn).await
    }

    async fn rollback_transaction(conn: &mut PooledConnection<CM>) -> QueryResult<()> {
        TM::rollback_transaction(&mut **conn).await
    }

    async fn commit_transaction(conn: &mut PooledConnection<CM>) -> QueryResult<()> {
        TM::commit_transaction(&mut **conn).await
    }

    fn get_transaction_depth(conn: &mut PooledConnection<CM>) -> u32 {
        TM::get_transaction_depth(&mut **conn)
    }
}

/// The error used when managing connections with `deadpool`.
#[derive(Debug)]
pub enum Error {
    /// An error occurred establishing the connection
    ConnectionError(ConnectionError),

    /// An error occurred pinging the database
    QueryError(diesel::result::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Error::ConnectionError(ref e) => e.fmt(f),
            Error::QueryError(ref e) => e.fmt(f),
        }
    }
}

impl std::error::Error for Error {}
