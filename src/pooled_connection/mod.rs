//! This module contains support using diesel-async with
//! various async rust connection pooling solutions
//!
//! See the concrete pool implementations for examples:
//! * [deadpool](self::deadpool)
//! * [bb8](self::bb8)
//! * [mobc](self::mobc)

use crate::TransactionManager;
use crate::{AsyncConnection, AsyncConnectionGatWorkaround, SimpleAsyncConnection};
use std::fmt;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::sync::{Arc, Mutex};

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

/// An connection manager for use with diesel-async.
///
/// See the concrete pool implementations for examples:
/// * [deadpool](self::deadpool)
/// * [bb8](self::bb8)
/// * [mobc](self::mobc)
pub struct AsyncDieselConnectionManager<C> {
    // use arc/mutex here to make `C` always `Send` + `Sync`
    // so that this type is send + sync
    p: PhantomData<Arc<Mutex<C>>>,
    connection_url: String,
}

impl<C> AsyncDieselConnectionManager<C> {
    /// Returns a new connection manager,
    /// which establishes connections to the given database URL.
    pub fn new(connection_url: impl Into<String>) -> Self {
        Self {
            p: PhantomData,
            connection_url: connection_url.into(),
        }
    }
}

impl<'conn, 'query, C, DB> AsyncConnectionGatWorkaround<'conn, 'query, DB> for C
where
    DB: diesel::backend::Backend,
    C: DerefMut,
    C::Target: AsyncConnectionGatWorkaround<'conn, 'query, DB>,
{
    type ExecuteFuture =
        <C::Target as AsyncConnectionGatWorkaround<'conn, 'query, DB>>::ExecuteFuture;
    type LoadFuture = <C::Target as AsyncConnectionGatWorkaround<'conn, 'query, DB>>::LoadFuture;
    type Stream = <C::Target as AsyncConnectionGatWorkaround<'conn, 'query, DB>>::Stream;
    type Row = <C::Target as AsyncConnectionGatWorkaround<'conn, 'query, DB>>::Row;
}

#[async_trait::async_trait]
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

#[async_trait::async_trait]
impl<C> AsyncConnection for C
where
    C: DerefMut + Send,
    C::Target: AsyncConnection,
{
    type Backend = <C::Target as AsyncConnection>::Backend;

    type TransactionManager =
        PoolTransactionManager<<C::Target as AsyncConnection>::TransactionManager>;

    async fn establish(_database_url: &str) -> diesel::ConnectionResult<Self> {
        Err(diesel::result::ConnectionError::BadConnection(
            String::from("Cannot directly establish a pooled connection"),
        ))
    }

    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> <Self as crate::AsyncConnectionGatWorkaround<'conn, 'query, Self::Backend>>::LoadFuture
    where
        T: diesel::query_builder::AsQuery + Send + 'query,
        T::Query: diesel::query_builder::QueryFragment<Self::Backend>
            + diesel::query_builder::QueryId
            + Send
            + 'query,
    {
        let conn = self.deref_mut();
        conn.load(source)
    }

    fn execute_returning_count<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> <Self as crate::AsyncConnectionGatWorkaround<'conn, 'query, Self::Backend>>::ExecuteFuture
    where
        T: diesel::query_builder::QueryFragment<Self::Backend>
            + diesel::query_builder::QueryId
            + Send
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
}

#[doc(hidden)]
#[allow(missing_debug_implementations)]
pub struct PoolTransactionManager<TM>(std::marker::PhantomData<TM>);

#[async_trait::async_trait]
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

    fn get_transaction_depth(conn: &mut C) -> u32 {
        TM::get_transaction_depth(&mut **conn)
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
#[async_trait::async_trait]
pub trait PoolableConnection: AsyncConnection {
    /// Check if a connection is still valid
    ///
    /// The default implementation performs a `SELECT 1` query
    async fn ping(&mut self) -> diesel::QueryResult<()> {
        use crate::RunQueryDsl;
        CheckConnectionQuery.execute(self).await.map(|_| ())
    }

    /// Checks if the connection is broken and should not be reused
    ///
    /// This method should return only contain a fast non-blocking check
    /// if the connection is considered to be broken or not. See
    /// [ManageConnection::has_broken] for details.
    ///
    /// The default implementation does not consider any connection as broken
    fn is_broken(&self) -> bool {
        false
    }
}
