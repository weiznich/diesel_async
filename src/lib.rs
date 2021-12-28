use diesel::backend::Backend;
use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
use diesel::row::Row;
use diesel::{ConnectionResult, QueryResult};
use futures::future::BoxFuture;
use futures::Stream;
#[cfg(feature = "deadpool")]
mod deadpool;
pub use self::deadpool::{AsyncConnectionManager, DeadpoolConnection};
#[cfg(feature = "mysql")]
mod mysql;
#[cfg(feature = "postgres")]
mod pg;
mod run_query_dsl;
mod stmt_cache;
mod transaction_manager;

#[cfg(feature = "mysql")]
pub use self::mysql::AsyncMysqlConnection;
#[cfg(feature = "postgres")]
pub use self::pg::AsyncPgConnection;
pub use self::run_query_dsl::*;
pub use self::stmt_cache::StmtCache;
pub use self::transaction_manager::{AnsiTransactionManager, TransactionManager};

#[async_trait::async_trait]
pub trait SimpleAsyncConnection {
    async fn batch_execute(&mut self, query: &str) -> QueryResult<()>;
}

pub trait AsyncConnectionGatWorkaround<'a, DB: Backend> {
    type Stream: Stream<Item = QueryResult<Self::Row>> + Send + 'a;
    type Row: Row<'a, DB> + 'a;
}

#[async_trait::async_trait]
pub trait AsyncConnection: SimpleAsyncConnection + Sized + Send
where
    for<'a> Self: AsyncConnectionGatWorkaround<'a, Self::Backend>,
{
    type Backend: Backend;
    type TransactionManager: TransactionManager<Self>;

    async fn establish(database_url: &str) -> ConnectionResult<Self>;

    async fn execute(&mut self, query: &str) -> QueryResult<usize>;

    async fn load<'a, T>(
        &'a mut self,
        source: T,
    ) -> QueryResult<<Self as AsyncConnectionGatWorkaround<'a, Self::Backend>>::Stream>
    where
        T: AsQuery + Send,
        T::Query: QueryFragment<Self::Backend> + QueryId + Send;

    async fn execute_returning_count<T>(&mut self, source: T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId + Send;

    async fn transaction<F, R, E>(&mut self, callback: F) -> Result<R, E>
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

    async fn begin_test_transaction(&mut self) -> QueryResult<()> {
        assert_eq!(Self::TransactionManager::get_transaction_depth(self), 0);
        Self::TransactionManager::begin_transaction(self).await
    }

    fn transaction_state(
        &mut self,
    ) -> &mut <Self::TransactionManager as TransactionManager<Self>>::TransactionStateData;
}
