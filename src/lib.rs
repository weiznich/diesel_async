//! Diesel-async provides async variants of diesel releated query functionality
//!
//! diesel-async is an extension to diesel itself. It is designed to be used togehter
//! with the main diesel crate. It only provides async variants of core diesel traits,
//! that perform actual io-work.
//! This includes async counterparts the following traits:
//! * [`diesel::prelude::RunQueryDsl`](https://docs.diesel.rs/2.0.x/diesel/prelude/trait.RunQueryDsl.html)
//!    -> [`diesel_async::RunQueryDsl`](crate::RunQueryDsl)
//! * [`diesel::connection::Connection`](https://docs.diesel.rs/2.0.x/diesel/connection/trait.Connection.html)
//!    -> [`diesel_async::AsyncConnection`](crate::AsyncConnection)
//! * [`diesel::query_dsl::UpdateAndFetchResults`](https://docs.diesel.rs/2.0.x/diesel/query_dsl/trait.UpdateAndFetchResults.html)
//!    -> [`diesel_async::UpdateAndFetchResults`](crate::UpdateAndFetchResults)
//!
//! These traits closely mirror their diesel counter parts while providing async functionality.
//!
//! In addition to these core traits 2 fully async connection implementations are provided
//! by diesel-async:
//!
//! * [`AsyncMysqlConnection`] (enabled by the `mysql` feature)
//! * [`AsyncPgConnection`] (enabled by the `postgres` feature)
//!
//! Ordinary usage of `diesel-async` assumes that you just replace the corresponding sync trait
//! method calls and connections with their async counterparts.
//!
//! ```rust
//! # include!("./doctest_setup.rs");
//! #
//! diesel::table! {
//!    users(id) {
//!        id -> Integer,
//!        name -> Text,
//!    }
//! }
//! #
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! #     run_test().await;
//! # }
//! #
//! # async fn run_test() -> QueryResult<()> {
//! #     use diesel::insert_into;
//! use crate::users::dsl::*;
//! # let mut connection = establish_connection().await;
//! # /*
//! let mut connection = AsyncPgConnection::establish(std::env::var("DATABASE_URL")?).await?;
//! # */
//! let data = users
//!     // use ordinary diesel query dsl here
//!     .filter(id.gt(0))
//!     // execute the query via the provided
//!     // async variant of `RunQueryDsl`
//!     .load::<(i32, String)>(&mut connection)
//!     .await?;
//! let expected_data = vec![
//!     (1, String::from("Sean")),
//!     (2, String::from("Tess")),
//! ];
//! assert_eq!(expected_data, data);
//! #     Ok(())
//! # }
//! ```

#![warn(missing_docs)]
use diesel::backend::Backend;
use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
use diesel::row::Row;
use diesel::{ConnectionResult, QueryResult};
use futures::future::BoxFuture;
use futures::{Future, Stream};

#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "postgres")]
mod pg;
#[cfg(any(feature = "deadpool", feature = "bb8", feature = "mobc"))]
pub mod pooled_connection;
mod run_query_dsl;
mod stmt_cache;
mod transaction_manager;

#[cfg(feature = "mysql")]
pub use self::mysql::AsyncMysqlConnection;
#[cfg(feature = "postgres")]
pub use self::pg::AsyncPgConnection;
pub use self::run_query_dsl::*;

use self::transaction_manager::{AnsiTransactionManager, TransactionManager};

/// Perform simple operations on a backend.
///
/// You should likely use [`AsyncConnection`] instead.
#[async_trait::async_trait]
pub trait SimpleAsyncConnection {
    /// Execute multiple SQL statements within the same string.
    ///
    /// This function is used to execute migrations,
    /// which may contain more than one SQL statement.
    async fn batch_execute(&mut self, query: &str) -> QueryResult<()>;
}

/// This trait is a workaround to emulate GAT on stable rust
///
/// It is used to specify the return type of `AsyncConnection::load`
/// and `AsyncConnection::execute` which may contain lifetimes
pub trait AsyncConnectionGatWorkaround<'conn, 'query, DB: Backend> {
    /// The future returned by `AsyncConnection::execute`
    type ExecuteFuture: Future<Output = QueryResult<usize>> + Send;
    /// The future returned by `AsyncConnection::load`
    type LoadFuture: Future<Output = QueryResult<Self::Stream>> + Send;
    /// The inner stream returned by `AsyncConnection::load`
    type Stream: Stream<Item = QueryResult<Self::Row>> + Send;
    /// The row type used by the stream returned by `AsyncConnection::load`
    type Row: Row<'conn, DB>;
}

/// An async connection to a database
///
/// This trait represents a n async database connection. It can be used to query the database through
/// the query dsl provided by diesel, custom extensions or raw sql queries. It essentially mirrors
/// the sync diesel [`Connection`](diesel::connection::Connection) implementation
#[async_trait::async_trait]
pub trait AsyncConnection: SimpleAsyncConnection + Sized + Send
where
    for<'a, 'b> Self: AsyncConnectionGatWorkaround<'a, 'b, Self::Backend>,
{
    /// The backend this type connects to
    type Backend: Backend;

    #[doc(hidden)]
    type TransactionManager: TransactionManager<Self>;

    /// Establishes a new connection to the database
    ///
    /// The argument to this method and the method's behavior varies by backend.
    /// See the documentation for that backend's connection class
    /// for details about what it accepts and how it behaves.
    async fn establish(database_url: &str) -> ConnectionResult<Self>;

    /// Executes the given function inside of a database transaction
    ///
    /// This function executes the provided closure `f` inside a database
    /// transaction. If there is already an open transaction for the current
    /// connection savepoints will be used instead. The connection is commited if
    /// the closure returns `Ok(_)`, it will be rolled back if it returns `Err(_)`.
    /// For both cases the original result value will be returned from this function.
    ///
    /// If the transaction fails to commit due to a `SerializationFailure` or a
    /// `ReadOnlyTransaction` a rollback will be attempted. In this case a
    /// [`Error::CommitTransactionFailed`](crate::result::Error::CommitTransactionFailed)
    /// error is returned, which contains details about the original error and
    /// the success of the rollback attempt.
    /// If the rollback failed the connection should be considered broken
    /// as it contains a uncommitted unabortable open transaction. Any further
    /// interaction with the transaction system will result in an returned error
    /// in this cases.
    ///
    /// If the closure returns an `Err(_)` and the rollback fails the function
    /// will return a [`Error::RollbackError`](crate::result::Error::RollbackError)
    /// wrapping the error generated by the rollback operation instead.
    /// In this case the connection should be considered broken as it contains
    /// an unabortable open transaction.
    ///
    /// If a nested transaction fails to release the corresponding savepoint
    /// a rollback will be attempted.  In this case a
    /// [`Error::CommitTransactionFailed`](crate::result::Error::CommitTransactionFailed)
    /// error is returned, which contains the original error and
    /// details about the success of the rollback attempt.
    ///
    /// # Example
    ///
    /// ```rust
    /// # include!("doctest_setup.rs");
    /// use diesel::result::Error;
    /// use futures::FutureExt;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await.unwrap();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use schema::users::dsl::*;
    /// #     let conn = &mut establish_connection().await;
    /// conn.transaction::<_, Error, _>(|conn| async move {
    ///     diesel::insert_into(users)
    ///         .values(name.eq("Ruby"))
    ///         .execute(conn)
    ///         .await?;
    ///
    ///     let all_names = users.select(name).load::<String>(conn).await?;
    ///     assert_eq!(vec!["Sean", "Tess", "Ruby"], all_names);
    ///
    ///     Ok(())
    /// }.boxed()).await?;
    ///
    /// conn.transaction::<(), _, _>(|conn| async move {
    ///     diesel::insert_into(users)
    ///         .values(name.eq("Pascal"))
    ///         .execute(conn)
    ///         .await?;
    ///
    ///     let all_names = users.select(name).load::<String>(conn).await?;
    ///     assert_eq!(vec!["Sean", "Tess", "Ruby", "Pascal"], all_names);
    ///
    ///     // If we want to roll back the transaction, but don't have an
    ///     // actual error to return, we can return `RollbackTransaction`.
    ///     Err(Error::RollbackTransaction)
    /// }.boxed()).await;
    ///
    /// let all_names = users.select(name).load::<String>(conn).await?;
    /// assert_eq!(vec!["Sean", "Tess", "Ruby"], all_names);
    /// #     Ok(())
    /// # }
    /// ```
    async fn transaction<R, E, F>(&mut self, callback: F) -> Result<R, E>
    where
        F: FnOnce(&mut Self) -> BoxFuture<Result<R, E>> + Send,
        E: From<diesel::result::Error> + Send,
        R: Send,
    {
        Self::TransactionManager::begin_transaction(self).await?;
        match callback(&mut *self).await {
            Ok(value) => {
                Self::TransactionManager::commit_transaction(self).await?;
                Ok(value)
            }
            Err(e) => {
                Self::TransactionManager::rollback_transaction(self)
                    .await
                    .map_err(|e| diesel::result::Error::RollbackError(Box::new(e)))?;
                Err(e)
            }
        }
    }

    /// Creates a transaction that will never be committed. This is useful for
    /// tests. Panics if called while inside of a transaction or
    /// if called with a connection containing a broken transaction
    async fn begin_test_transaction(&mut self) -> QueryResult<()> {
        assert_eq!(Self::TransactionManager::get_transaction_depth(self), 0);
        Self::TransactionManager::begin_transaction(self).await
    }

    #[doc(hidden)]
    fn load<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> <Self as AsyncConnectionGatWorkaround<'conn, 'query, Self::Backend>>::LoadFuture
    where
        T: AsQuery + Send + 'query,
        T::Query: QueryFragment<Self::Backend> + QueryId + Send + 'query;

    #[doc(hidden)]
    fn execute_returning_count<'conn, 'query, T>(
        &'conn mut self,
        source: T,
    ) -> <Self as AsyncConnectionGatWorkaround<'conn, 'query, Self::Backend>>::ExecuteFuture
    where
        T: QueryFragment<Self::Backend> + QueryId + Send + 'query;

    #[doc(hidden)]
    fn transaction_state(
        &mut self,
    ) -> &mut <Self::TransactionManager as TransactionManager<Self>>::TransactionStateData;
}
