//! A connection pool implementation for `diesel-async` based on [`deadpool`]
//!
//! ```rust
//! # include!("../doctest_setup.rs");
//! use diesel::result::Error;
//! use futures_util::FutureExt;
//! use diesel_async::pooled_connection::AsyncDieselConnectionManager;
//! use diesel_async::pooled_connection::deadpool::Pool;
//! use diesel_async::RunQueryDsl;
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! #     run_test().await.unwrap();
//! # }
//! #
//! # #[cfg(feature = "postgres")]
//! # fn get_config() -> AsyncDieselConnectionManager<diesel_async::AsyncPgConnection> {
//! #     let db_url = database_url_from_env("PG_DATABASE_URL");
//! let config = AsyncDieselConnectionManager::<diesel_async::AsyncPgConnection>::new(db_url);
//! #     config
//! #  }
//! #
//! # #[cfg(feature = "mysql")]
//! # fn get_config() -> AsyncDieselConnectionManager<diesel_async::AsyncMysqlConnection> {
//! #     let db_url = database_url_from_env("MYSQL_DATABASE_URL");
//! #    let config = AsyncDieselConnectionManager::<diesel_async::AsyncMysqlConnection>::new(db_url);
//! #     config
//! #  }
//! #
//! # async fn run_test() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
//! #     use schema::users::dsl::*;
//! #     let config = get_config();
//! let pool = Pool::builder(config).build()?;
//! let mut conn = pool.get().await?;
//! # conn.begin_test_transaction();
//! # clear_tables(&mut conn).await;
//! # create_tables(&mut conn).await;
//! # conn.begin_test_transaction();
//! let res = users.load::<(i32, String)>(&mut conn).await?;
//! #     Ok(())
//! # }
//! ```
use super::{AsyncDieselConnectionManager, PoolableConnection};
use deadpool::managed::Manager;

/// Type alias for using [`deadpool::managed::Pool`] with [`diesel-async`]
pub type Pool<C> = deadpool::managed::Pool<AsyncDieselConnectionManager<C>>;
/// Type alias for using [`deadpool::managed::PoolBuilder`] with [`diesel-async`]
pub type PoolBuilder<C> = deadpool::managed::PoolBuilder<AsyncDieselConnectionManager<C>>;
/// Type alias for using [`deadpool::managed::BuildError`] with [`diesel-async`]
pub type BuildError = deadpool::managed::BuildError<super::PoolError>;
/// Type alias for using [`deadpool::managed::PoolError`] with [`diesel-async`]
pub type PoolError = deadpool::managed::PoolError<super::PoolError>;
/// Type alias for using [`deadpool::managed::Object`] with [`diesel-async`]
pub type Object<C> = deadpool::managed::Object<AsyncDieselConnectionManager<C>>;
/// Type alias for using [`deadpool::managed::Hook`] with [`diesel-async`]
pub type Hook<C> = deadpool::managed::Hook<AsyncDieselConnectionManager<C>>;
/// Type alias for using [`deadpool::managed::HookError`] with [`diesel-async`]
pub type HookError = deadpool::managed::HookError<super::PoolError>;
/// Type alias for using [`deadpool::managed::HookErrorCause`] with [`diesel-async`]
pub type HookErrorCause = deadpool::managed::HookErrorCause<super::PoolError>;

#[async_trait::async_trait]
impl<C> Manager for AsyncDieselConnectionManager<C>
where
    C: PoolableConnection + Send + 'static,
{
    type Type = C;

    type Error = super::PoolError;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        (self.setup)(&self.connection_url)
            .await
            .map_err(super::PoolError::ConnectionError)
    }

    async fn recycle(&self, obj: &mut Self::Type) -> deadpool::managed::RecycleResult<Self::Error> {
        if std::thread::panicking() || obj.is_broken() {
            return Err(deadpool::managed::RecycleError::StaticMessage(
                "Broken connection",
            ));
        }
        obj.ping().await.map_err(super::PoolError::QueryError)?;
        Ok(())
    }
}
