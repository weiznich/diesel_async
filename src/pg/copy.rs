//! Postgres COPY FROM support
//!
//! This module provides asynchronous support for PostgreSQL's `COPY FROM` command.
//! It adapters Diesel's synchronous `COPY FROM` DSL to a `tokio-postgres` based backend.
//!
//! # Usage
//!
//! For most use cases, you can use [`diesel::copy_from`](https://docs.diesel.rs/2.2.x/diesel/fn.copy_from.html)
//! to construct a query and then use the [`AsyncExecuteCopyFromDsl::execute`] method:
//!
//! ```rust
//! # use diesel_async::RunQueryDsl;
//! # use diesel::dsl::copy_from;
//! # use diesel_async::pg::copy::AsyncExecuteCopyFromDsl;
//! # use std::io::Write;
//! # async fn run_example(conn: &mut diesel_async::AsyncPgConnection) -> diesel::QueryResult<usize> {
//! # diesel::table! { users { id -> Integer, name -> Text, } }
//! let result = copy_from(users::table)
//!     .from_raw_data((users::name,), |writer: &mut dyn Write| {
//!         writeln!(writer, "Alice")?;
//!         writeln!(writer, "Bob")?;
//!         Ok::<(), std::io::Error>(())
//!     })
//!     .execute(conn)
//!     .await?;
//! # Ok(result)
//! # }
//! ```
//!
//! You can also use [`from_insertable`](https://docs.diesel.rs/2.2.x/diesel/pg/query_builder/copy/struct.CopyFromQuery.html#method.from_insertable)
//! to copy a bulk of data into the database:
//!
//! ```rust
//! # use diesel_async::RunQueryDsl;
//! # use diesel::dsl::copy_from;
//! # use diesel_async::pg::copy::AsyncExecuteCopyFromDsl;
//! # use diesel::prelude::*;
//! # async fn run_example(conn: &mut diesel_async::AsyncPgConnection) -> diesel::QueryResult<usize> {
//! # diesel::table! { users { id -> Integer, name -> Text, } }
//! # #[derive(Insertable)]
//! # #[diesel(table_name = users)]
//! # struct NewUser<'a> { name: &'a str }
//! let data = vec![NewUser { name: "Alice" }, NewUser { name: "Bob" }];
//! let result = copy_from(users::table)
//!     .from_insertable(data)
//!     .execute(conn)
//!     .await?;
//! # Ok(result)
//! # }
//! ```
//!
//! # Memory Buffering
//!
//! > [!IMPORTANT]
//! > Due to the synchronous nature of Diesel's `CopyFromExpression` trait, this implementation
//! > currently **buffers the entire payload in memory** before sending it to the database.
//! >
//! > For extremely large datasets (multi-gigabyte copies), this may lead to high memory consumption.
//! > True zero-copy streaming support requires an asynchronous callback trait which may be added in
//! > the future.
//!
//! # Advanced usage
//!
//! If you need more control over how data is fed into the database, you can use
//! [`AsyncPgConnection::copy_in`](crate::pg::AsyncPgConnection::copy_in) directly.

use crate::AsyncPgConnection;
use diesel::pg::{CopyFromExpression, CopyFromQuery, CopyTarget, Pg};
use diesel::query_builder::{AstPass, QueryBuilder, QueryFragment};
use diesel::QueryResult;
use futures_core::future::BoxFuture;
use futures_util::FutureExt;

// -------------------------------------------------------------
// Compatibility for diesel's CopyFromQuery (buffers in memory to handle sync constraints)
// -------------------------------------------------------------

struct AsyncInternalCopyFromQuery<'a, T, Action> {
    _target: std::marker::PhantomData<&'a T>,
    action: &'a Action,
}

impl<'a, T, Action> QueryFragment<Pg> for AsyncInternalCopyFromQuery<'a, T, Action>
where
    T: CopyTarget,
    Action: CopyFromExpression<T>,
{
    fn walk_ast<'b>(&'b self, mut pass: AstPass<'_, 'b, Pg>) -> QueryResult<()> {
        pass.unsafe_to_cache_prepared();
        pass.push_sql("COPY ");
        self.action.walk_target(pass.reborrow())?;
        pass.push_sql(" FROM STDIN");
        QueryFragment::<Pg>::walk_ast(self.action.options(), pass.reborrow())?;
        Ok(())
    }
}

/// Trait to execute a `COPY FROM` statement asynchronously
pub trait AsyncExecuteCopyFromDsl<Conn> {
    /// Executes the `COPY FROM` statement.
    ///
    /// This will automatically buffer all data produced by the query's callback
    /// into memory and send it to the database.
    fn execute<'conn>(self, conn: &'conn mut Conn) -> BoxFuture<'conn, QueryResult<usize>>
    where
        Self: 'conn;
}

impl<T, Action> AsyncExecuteCopyFromDsl<AsyncPgConnection> for CopyFromQuery<T, Action>
where
    Action: CopyFromExpression<T> + Send,
    <Action as CopyFromExpression<T>>::Error: Send + Sync + 'static,
    T: CopyTarget + Send,
{
    fn execute<'conn>(
        mut self,
        conn: &'conn mut AsyncPgConnection,
    ) -> BoxFuture<'conn, QueryResult<usize>>
    where
        Self: 'conn,
    {
        let internal_query = AsyncInternalCopyFromQuery::<T, Action> {
            _target: std::marker::PhantomData,
            action: &self.action,
        };

        let mut qb = diesel::pg::PgQueryBuilder::default();
        let sql = match QueryFragment::<Pg>::to_sql(&internal_query, &mut qb, &Pg) {
            Ok(_) => qb.finish(),
            Err(e) => return futures_util::future::err(e).boxed(),
        };

        async move {
            let sink = conn.copy_in(&sql).await?;

            // Because diesel's action.callback() is synchronous and takes `&mut impl Write`,
            // we buffer the entire payload in memory here.
            // If we used spawn_blocking, we'd require T and Action to be 'static, which
            // breaks `from_insertable` for borrowed structs.
            let mut buffer = Vec::new();
            if let Err(e) = self.action.callback(&mut buffer) {
                return Err(diesel::result::Error::SerializationError(Box::new(e) as _));
            }

            use futures_util::SinkExt;
            tokio::pin!(sink);

            sink.send(bytes::Bytes::from(buffer))
                .await
                .map_err(|e| diesel::result::Error::SerializationError(Box::new(e) as _))?;

            let rows = sink
                .finish()
                .await
                .map_err(|e| diesel::result::Error::SerializationError(Box::new(e) as _))?;

            rows.try_into()
                .map_err(|e| diesel::result::Error::DeserializationError(Box::new(e) as _))
        }
        .boxed()
    }
}
