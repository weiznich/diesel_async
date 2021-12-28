use core::fmt;
use std::marker::PhantomData;

use async_trait::async_trait;
use deadpool::managed as deadpool;
use diesel::{
    query_builder::{AsQuery, QueryFragment, QueryId},
    ConnectionError, ConnectionResult, QueryResult,
};

use crate::{
    AsyncConnection, AsyncConnectionGatWorkaround, SimpleAsyncConnection, TransactionManager,
};

/// An deadpool connection manager for use with async Diesel.
///
/// See the [deadpool documentation] for usage examples.
///
/// [deadpool documentation]: deadpool
pub struct AsyncConnectionManager<T> {
    // TODO: Ideally there should be common statement cache.
    database_url: String,
    _marker: PhantomData<T>,
}

unsafe impl<T: Send> Sync for AsyncConnectionManager<T> {}

impl<T: AsyncConnection> fmt::Debug for AsyncConnectionManager<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AsyncConnectionManager<{}>", std::any::type_name::<T>())
    }
}

impl<T> AsyncConnectionManager<T> {
    /// Returns a new connection manager,
    /// which establishes connections to the given database URL.
    pub fn new<S: Into<String>>(database_url: S) -> Self {
        AsyncConnectionManager {
            database_url: database_url.into(),
            _marker: PhantomData,
        }
    }
}

/// A trait indicating a connection could be used inside a deadpool pool
// TODO: ping with query can be expensive for some use cases
#[async_trait]
pub trait DeadpoolConnection: AsyncConnection {
    /// Check if a connection is still valid
    async fn recycle(&mut self) -> QueryResult<()>;
}

#[cfg(feature = "postgres")]
#[async_trait]
impl DeadpoolConnection for crate::pg::AsyncPgConnection {
    async fn recycle(&mut self) -> QueryResult<()> {
        self.execute("SELECT 1").await.map(|_| ())
    }
}

#[cfg(feature = "mysql")]
#[async_trait]
impl DeadpoolConnection for crate::mysql::AsyncMysqlConnection {
    async fn recycle(&mut self) -> QueryResult<()> {
        self.execute("SELECT 1").await.map(|_| ())
    }
}

#[async_trait]
impl<T: DeadpoolConnection> deadpool::Manager for AsyncConnectionManager<T> {
    type Type = T;
    type Error = Error;

    async fn create(&self) -> Result<T, Self::Error> {
        T::establish(&self.database_url)
            .await
            .map_err(Error::ConnectionError)
    }

    async fn recycle(&self, conn: &mut T) -> deadpool::RecycleResult<Self::Error> {
        conn.recycle()
            .await
            .map_err(|e| deadpool::RecycleError::Backend(Error::QueryError(e)))
    }

    fn detach(&self, _obj: &mut Self::Type) {
        // TODO: Can be used to detach from common cache
    }
}

#[async_trait::async_trait]
impl<T> SimpleAsyncConnection for deadpool::Object<T>
where
    T: deadpool::Manager,
    T::Type: AsyncConnection,
{
    async fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        (&mut **self).batch_execute(query).await
    }
}

impl<'a, T> AsyncConnectionGatWorkaround<'a, <T::Type as AsyncConnection>::Backend>
    for deadpool::Object<T>
where
    T: deadpool::Manager,
    T::Type: AsyncConnection,
{
    type Stream = <T::Type as AsyncConnectionGatWorkaround<
        'a,
        <T::Type as AsyncConnection>::Backend,
    >>::Stream;

    type Row =
        <T::Type as AsyncConnectionGatWorkaround<'a, <T::Type as AsyncConnection>::Backend>>::Row;
}

#[async_trait::async_trait]
impl<T> AsyncConnection for deadpool::Object<T>
where
    T: deadpool::Manager,
    T::Type: AsyncConnection,
{
    type Backend = <T::Type as AsyncConnection>::Backend;

    type TransactionManager =
        PoolTransactionManager<<T::Type as AsyncConnection>::TransactionManager>;

    async fn establish(_: &str) -> ConnectionResult<Self> {
        Err(ConnectionError::BadConnection(String::from(
            "Cannot directly establish a pooled connection",
        )))
    }

    async fn execute(&mut self, query: &str) -> QueryResult<usize> {
        (&mut **self).execute(query).await
    }

    async fn load<'a, S>(
        &'a mut self,
        source: S,
    ) -> QueryResult<<Self as AsyncConnectionGatWorkaround<'a, Self::Backend>>::Stream>
    where
        S: AsQuery + Send,
        S::Query: QueryFragment<Self::Backend> + QueryId + Send,
    {
        (&mut **self).load(source).await
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
pub struct PoolTransactionManager<T>(std::marker::PhantomData<T>);

#[async_trait::async_trait]
impl<T, TM> TransactionManager<deadpool::Object<T>> for PoolTransactionManager<TM>
where
    T: deadpool::Manager,
    T::Type: AsyncConnection<TransactionManager = TM>,
    TM: TransactionManager<T::Type>,
{
    type TransactionStateData = TM::TransactionStateData;

    async fn begin_transaction(conn: &mut deadpool::Object<T>) -> QueryResult<()> {
        TM::begin_transaction(&mut **conn).await
    }

    async fn rollback_transaction(conn: &mut deadpool::Object<T>) -> QueryResult<()> {
        TM::rollback_transaction(&mut **conn).await
    }

    async fn commit_transaction(conn: &mut deadpool::Object<T>) -> QueryResult<()> {
        TM::commit_transaction(&mut **conn).await
    }

    fn get_transaction_depth(conn: &mut deadpool::Object<T>) -> u32 {
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
