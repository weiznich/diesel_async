use std::pin::Pin;

use crate::AsyncConnection;
use diesel::associations::HasTable;
use diesel::backend::Backend;
use diesel::deserialize::FromSqlRow;
#[cfg(any(feature = "mysql", feature = "postgres"))]
use diesel::expression::{is_aggregate, MixedAggregates, ValidGrouping};
use diesel::query_builder::IntoUpdateTarget;
#[cfg(any(feature = "mysql", feature = "postgres"))]
use diesel::query_source::QuerySource;
use diesel::result::QueryResult;
use diesel::AsChangeset;
#[cfg(any(feature = "mysql", feature = "postgres"))]
use diesel::{dsl, Table};
use futures::{Stream, StreamExt, TryStreamExt};

pub mod methods {
    use super::*;
    use crate::AsyncConnectionGatWorkaround;
    use diesel::backend::Backend;
    use diesel::deserialize::FromSqlRow;
    use diesel::expression::QueryMetadata;
    use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
    use diesel::query_dsl::CompatibleType;
    use futures::{Stream, StreamExt};

    #[async_trait::async_trait]
    pub trait ExecuteDsl<Conn, DB = <Conn as AsyncConnection>::Backend>
    where
        Conn: AsyncConnection<Backend = DB>,
        DB: Backend,
    {
        async fn execute(query: Self, conn: &mut Conn) -> QueryResult<usize>;
    }

    #[async_trait::async_trait]
    impl<Conn, DB, T> ExecuteDsl<Conn, DB> for T
    where
        Conn: AsyncConnection<Backend = DB>,
        DB: Backend,
        T: QueryFragment<DB> + QueryId + Send,
    {
        async fn execute(query: Self, conn: &mut Conn) -> QueryResult<usize> {
            conn.execute_returning_count(query).await
        }
    }

    pub trait LoadQueryGatWorkaround<'a, Conn, U> {
        type Stream: Stream<Item = QueryResult<U>> + Send + 'a;
    }

    #[async_trait::async_trait]
    pub trait LoadQuery<Conn: AsyncConnection, U>
    where
        for<'a> Self: LoadQueryGatWorkaround<'a, Conn, U>,
    {
        async fn internal_load<'a>(
            self,
            conn: &'a mut Conn,
        ) -> QueryResult<<Self as LoadQueryGatWorkaround<'a, Conn, U>>::Stream>;
    }

    impl<'a, Conn, U, T, DB, ST> LoadQueryGatWorkaround<'a, Conn, U> for T
    where
        Conn: AsyncConnection<Backend = DB>,
        T: AsQuery + Send,
        T::Query: QueryFragment<DB> + QueryId + Send,
        T::SqlType: CompatibleType<U, DB, SqlType = ST>,
        U: FromSqlRow<ST, DB> + Send + 'static,
        DB: QueryMetadata<T::SqlType>,
    {
        type Stream = futures::stream::Map<
            <Conn as AsyncConnectionGatWorkaround<'a, DB>>::Stream,
            fn(QueryResult<<Conn as AsyncConnectionGatWorkaround<'a, DB>>::Row>) -> QueryResult<U>,
        >;
    }

    #[async_trait::async_trait]
    impl<Conn, DB, T, U, ST> LoadQuery<Conn, U> for T
    where
        Conn: AsyncConnection<Backend = DB>,
        DB: Backend + 'static,
        T: AsQuery + Send,
        T::Query: QueryFragment<DB> + QueryId + Send,
        T::SqlType: CompatibleType<U, DB, SqlType = ST>,
        U: FromSqlRow<ST, DB> + Send + 'static,
        DB: QueryMetadata<T::SqlType>,
        ST: 'static,
    {
        async fn internal_load<'a>(
            self,
            conn: &'a mut Conn,
        ) -> QueryResult<<Self as LoadQueryGatWorkaround<'a, Conn, U>>::Stream> {
            Ok(conn.load(self).await?.map(map_row_helper::<_, DB, U, ST>))
        }
    }
}

fn map_row_helper<'a, R, DB, U, ST>(row: QueryResult<R>) -> QueryResult<U>
where
    U: FromSqlRow<ST, DB>,
    R: diesel::row::Row<'a, DB>,
    DB: Backend,
{
    U::build_from_row(&row?).map_err(diesel::result::Error::DeserializationError)
}

#[async_trait::async_trait]
pub trait RunQueryDsl<Conn>: Sized {
    /// Executes the given command, returning the number of rows affected.
    ///
    /// `execute` is usually used in conjunction with [`insert_into`](diesel::insert_into()),
    /// [`update`](diesel::update()) and [`delete`](diesel::delete()) where the number of
    /// affected rows is often enough information.
    ///
    /// When asking the database to return data from a query, [`load`](crate::run_query_dsl::RunQueryDsl::load()) should
    /// probably be used instead.
    ///
    /// # Example
    ///
    /// ```rust
    /// # include!("../doctest_setup.rs");
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await;
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::insert_into;
    /// #     use schema::users::dsl::*;
    /// #     let connection = &mut establish_connection().await;
    /// let inserted_rows = insert_into(users)
    ///     .values(name.eq("Ruby"))
    ///     .execute(connection)
    ///     .await?;
    /// assert_eq!(1, inserted_rows);
    ///
    /// let inserted_rows = insert_into(users)
    ///     .values(&vec![name.eq("Jim"), name.eq("James")])
    ///     .execute(connection)
    ///     .await?;
    /// assert_eq!(2, inserted_rows);
    /// #     Ok(())
    /// # }
    /// ```
    async fn execute(self, conn: &mut Conn) -> QueryResult<usize>
    where
        Conn: AsyncConnection + Send,
        Self: methods::ExecuteDsl<Conn>,
    {
        methods::ExecuteDsl::execute(self, conn).await
    }

    /// Executes the given query, returning a [`Vec`] with the returned rows.
    ///
    /// When using the query builder, the return type can be
    /// a tuple of the values, or a struct which implements [`Queryable`].
    ///
    /// When this method is called on [`sql_query`],
    /// the return type can only be a struct which implements [`QueryableByName`]
    ///
    /// For insert, update, and delete operations where only a count of affected is needed,
    /// [`execute`] should be used instead.
    ///
    /// [`Queryable`]: diesel::deserialize::Queryable
    /// [`QueryableByName`]: diesel::deserialize::QueryableByName
    /// [`execute`]: crate::run_query_dsl::RunQueryDsl::execute()
    /// [`sql_query`]: diesel::sql_query()
    ///
    /// # Examples
    ///
    /// ## Returning a single field
    ///
    /// ```rust
    /// # include!("../doctest_setup.rs");
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await;
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::insert_into;
    /// #     use schema::users::dsl::*;
    /// #     let connection = &mut establish_connection().await;
    /// let data = users.select(name)
    ///     .load::<String>(connection)
    ///     .await?;
    /// assert_eq!(vec!["Sean", "Tess"], data);
    /// #     Ok(())
    /// # }
    /// ```
    ///
    /// ## Returning a tuple
    ///
    /// ```rust
    /// # include!("../doctest_setup.rs");
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await;
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::insert_into;
    /// #     use schema::users::dsl::*;
    /// #     let connection = &mut establish_connection().await;
    /// let data = users
    ///     .load::<(i32, String)>(connection)
    ///     .await?;
    /// let expected_data = vec![
    ///     (1, String::from("Sean")),
    ///     (2, String::from("Tess")),
    /// ];
    /// assert_eq!(expected_data, data);
    /// #     Ok(())
    /// # }
    /// ```
    ///
    /// ## Returning a struct
    ///
    /// ```rust
    /// # include!("../doctest_setup.rs");
    /// #
    /// #[derive(Queryable, PartialEq, Debug)]
    /// struct User {
    ///     id: i32,
    ///     name: String,
    /// }
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await;
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::insert_into;
    /// #     use schema::users::dsl::*;
    /// #     let connection = &mut establish_connection().await;
    /// let data = users
    ///     .load::<User>(connection)
    ///     .await?;
    /// let expected_data = vec![
    ///     User { id: 1, name: String::from("Sean") },
    ///     User { id: 2, name: String::from("Tess") },
    /// ];
    /// assert_eq!(expected_data, data);
    /// #     Ok(())
    /// # }
    /// ```
    async fn load<U>(self, conn: &mut Conn) -> QueryResult<Vec<U>>
    where
        U: Send,
        Conn: AsyncConnection,
        Self: methods::LoadQuery<Conn, U>,
    {
        let stream = self.internal_load(conn).await?;

        stream
            .try_fold(Vec::new(), |mut acc, item| {
                acc.push(item);
                futures::future::ready(Ok(acc))
            })
            .await
    }

    /// Executes the given query, returning a [`Stream`] with the returned rows.
    ///
    /// **You should normally prefer to use [`RunQueryDsl::load`] instead**. This method
    /// is provided for situations where the result needs to be collected into a different
    /// container than a [`Vec`]
    ///
    /// When using the query builder, the return type can be
    /// a tuple of the values, or a struct which implements [`Queryable`].
    ///
    /// When this method is called on [`sql_query`],
    /// the return type can only be a struct which implements [`QueryableByName`]
    ///
    /// For insert, update, and delete operations where only a count of affected is needed,
    /// [`execute`] should be used instead.
    ///
    /// [`Queryable`]: diesel::deserialize::Queryable
    /// [`QueryableByName`]: diesel::deserialize::QueryableByName
    /// [`execute`]: crate::run_query_dsl::RunQueryDsl::execute()
    /// [`sql_query`]: diesel::sql_query()
    ///
    /// # Examples
    ///
    /// ## Returning a single field
    ///
    /// ```rust
    /// # include!("../doctest_setup.rs");
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await;
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::insert_into;
    /// #     use schema::users::dsl::*;
    /// #     use futures::stream::TryStreamExt;
    /// #     let connection = &mut establish_connection().await;
    /// let data = users.select(name)
    ///     .load_stream::<String>(connection)
    ///     .await?
    ///     .try_fold(Vec::new(), |mut acc, item| {
    ///          acc.push(item);
    ///          futures::future::ready(Ok(acc))
    ///      })
    ///     .await?;
    /// assert_eq!(vec!["Sean", "Tess"], data);
    /// #     Ok(())
    /// # }
    /// ```
    ///
    /// ## Returning a tuple
    ///
    /// ```rust
    /// # include!("../doctest_setup.rs");
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await;
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::insert_into;
    /// #     use schema::users::dsl::*;
    /// #     use futures::stream::TryStreamExt;
    /// #     let connection = &mut establish_connection().await;
    /// let data = users
    ///     .load_stream::<(i32, String)>(connection)
    ///     .await?
    ///     .try_fold(Vec::new(), |mut acc, item| {
    ///          acc.push(item);
    ///          futures::future::ready(Ok(acc))
    ///      })
    ///     .await?;
    /// let expected_data = vec![
    ///     (1, String::from("Sean")),
    ///     (2, String::from("Tess")),
    /// ];
    /// assert_eq!(expected_data, data);
    /// #     Ok(())
    /// # }
    /// ```
    ///
    /// ## Returning a struct
    ///
    /// ```rust
    /// # include!("../doctest_setup.rs");
    /// #
    /// #[derive(Queryable, PartialEq, Debug)]
    /// struct User {
    ///     id: i32,
    ///     name: String,
    /// }
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await;
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::insert_into;
    /// #     use schema::users::dsl::*;
    /// #     use futures::stream::TryStreamExt;
    /// #     let connection = &mut establish_connection().await;
    /// let data = users
    ///     .load_stream::<User>(connection)
    ///     .await?
    ///     .try_fold(Vec::new(), |mut acc, item| {
    ///          acc.push(item);
    ///          futures::future::ready(Ok(acc))
    ///      })
    ///     .await?;
    /// let expected_data = vec![
    ///     User { id: 1, name: String::from("Sean") },
    ///     User { id: 2, name: String::from("Tess") },
    /// ];
    /// assert_eq!(expected_data, data);
    /// #     Ok(())
    /// # }
    /// ```
    async fn load_stream<'a, U>(
        self,
        conn: &'a mut Conn,
    ) -> QueryResult<Pin<Box<dyn Stream<Item = QueryResult<U>> + Send + 'a>>>
    where
        Conn: AsyncConnection,
        U: 'a,
        Self: methods::LoadQuery<Conn, U> + 'a,
    {
        self.internal_load(conn).await.map(|s| s.boxed())
    }

    /// Runs the command, and returns the affected row.
    ///
    /// `Err(NotFound)` will be returned if the query affected 0 rows. You can
    /// call `.optional()` on the result of this if the command was optional to
    /// get back a `Result<Option<U>>`
    ///
    /// When this method is called on an insert, update, or delete statement,
    /// it will implicitly add a `RETURNING *` to the query,
    /// unless a returning clause was already specified.
    ///
    /// This method only returns the first row that was affected, even if more
    /// rows are affected.
    ///
    /// # Example
    ///
    /// ```rust
    /// # include!("../doctest_setup.rs");
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await;
    /// # }
    /// #
    /// # #[cfg(feature = "postgres")]
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::{insert_into, update};
    /// #     use schema::users::dsl::*;
    /// #     let connection = &mut establish_connection().await;
    /// let inserted_row = insert_into(users)
    ///     .values(name.eq("Ruby"))
    ///     .get_result(connection)
    ///     .await?;
    /// assert_eq!((3, String::from("Ruby")), inserted_row);
    ///
    /// // This will return `NotFound`, as there is no user with ID 4
    /// let update_result = update(users.find(4))
    ///     .set(name.eq("Jim"))
    ///     .get_result::<(i32, String)>(connection)
    ///     .await;
    /// assert_eq!(Err(diesel::NotFound), update_result);
    /// #     Ok(())
    /// # }
    /// #
    /// # #[cfg(not(feature = "postgres"))]
    /// # async fn run_test() -> QueryResult<()> {
    /// #     Ok(())
    /// # }
    /// ```
    async fn get_result<U>(self, conn: &mut Conn) -> QueryResult<U>
    where
        U: Send,
        Conn: AsyncConnection,
        Self: methods::LoadQuery<Conn, U>,
    {
        let res = self.load(conn).await?;
        match res.into_iter().next() {
            Some(v) => Ok(v),
            None => Err(diesel::result::Error::NotFound),
        }
    }

    /// Runs the command, returning an `Vec` with the affected rows.
    ///
    /// This method is an alias for [`load`], but with a name that makes more
    /// sense for insert, update, and delete statements.
    ///
    /// [`load`]: crate::run_query_dsl::RunQueryDsl::load()
    async fn get_results<U>(self, conn: &mut Conn) -> QueryResult<Vec<U>>
    where
        U: Send,
        Conn: AsyncConnection,
        Self: methods::LoadQuery<Conn, U>,
    {
        self.load(conn).await
    }

    /// Attempts to load a single record.
    ///
    /// This method is equivalent to `.limit(1).get_result()`
    ///
    /// Returns `Ok(record)` if found, and `Err(NotFound)` if no results are
    /// returned. If the query truly is optional, you can call `.optional()` on
    /// the result of this to get a `Result<Option<U>>`.
    ///
    /// # Example:
    ///
    /// ```rust
    /// # include!("../doctest_setup.rs");
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use schema::users::dsl::*;
    /// #     let connection = &mut establish_connection().await;
    /// diesel::insert_into(users)
    ///     .values(&vec![name.eq("Sean"), name.eq("Pascal")])
    ///     .execute(connection)
    ///     .await?;
    ///
    /// let first_name = users.order(id)
    ///     .select(name)
    ///     .first(connection)
    ///     .await;
    /// assert_eq!(Ok(String::from("Sean")), first_name);
    ///
    /// let not_found = users
    ///     .filter(name.eq("Foo"))
    ///     .first::<(i32, String)>(connection)
    ///     .await;
    /// assert_eq!(Err(diesel::NotFound), not_found);
    /// #     Ok(())
    /// # }
    /// ```
    async fn first<U>(self, conn: &mut Conn) -> QueryResult<U>
    where
        U: Send,
        Conn: AsyncConnection,
        Self: diesel::query_dsl::methods::LimitDsl,
        diesel::dsl::Limit<Self>: methods::LoadQuery<Conn, U> + Send,
    {
        diesel::query_dsl::methods::LimitDsl::limit(self, 1)
            .get_result(conn)
            .await
    }
}

impl<T, Conn> RunQueryDsl<Conn> for T {}

#[async_trait::async_trait]
pub trait SaveChangesDsl<Conn> {
    async fn save_changes<T>(self, connection: &mut Conn) -> QueryResult<T>
    where
        Self: Sized,
        Conn: UpdateAndFetchResults<Self, T>,
    {
        connection.update_and_fetch(self).await
    }
}

impl<T, Conn> SaveChangesDsl<Conn> for T where
    T: Copy + AsChangeset<Target = <T as HasTable>::Table> + IntoUpdateTarget
{
}

#[async_trait::async_trait]
pub trait UpdateAndFetchResults<Changes, Output>: AsyncConnection {
    /// See the traits documentation.
    async fn update_and_fetch(&mut self, changeset: Changes) -> QueryResult<Output>
    where
        Changes: 'async_trait;
}

#[cfg(feature = "mysql")]
#[async_trait::async_trait]
impl<Changes, Output> UpdateAndFetchResults<Changes, Output> for crate::AsyncMysqlConnection
where
    Output: Send,
    Changes: Copy + diesel::Identifiable + Send,
    Changes: AsChangeset<Target = <Changes as HasTable>::Table> + IntoUpdateTarget,
    Changes::Table: diesel::query_dsl::methods::FindDsl<Changes::Id> + Send,
    Changes::WhereClause: Send,
    Changes::Changeset: Send,
    Changes::Id: Send,
    dsl::Update<Changes, Changes>: methods::ExecuteDsl<crate::AsyncMysqlConnection>,
    dsl::Find<Changes::Table, Changes::Id>:
        methods::LoadQuery<crate::AsyncMysqlConnection, Output> + Send,
    <Changes::Table as Table>::AllColumns: ValidGrouping<()>,
    <<Changes::Table as Table>::AllColumns as ValidGrouping<()>>::IsAggregate:
        MixedAggregates<is_aggregate::No, Output = is_aggregate::No>,
    <Changes::Table as QuerySource>::FromClause: Send,
{
    async fn update_and_fetch(&mut self, changeset: Changes) -> QueryResult<Output>
    where
        Changes: 'async_trait,
    {
        use diesel::query_dsl::methods::FindDsl;

        diesel::update(changeset)
            .set(changeset)
            .execute(self)
            .await?;
        Changes::table().find(changeset.id()).get_result(self).await
    }
}

#[cfg(feature = "postgres")]
#[async_trait::async_trait]
impl<Changes, Output> UpdateAndFetchResults<Changes, Output> for crate::AsyncPgConnection
where
    Output: Send,
    Changes: Copy + AsChangeset<Target = <Changes as HasTable>::Table> + IntoUpdateTarget + Send,
    dsl::Update<Changes, Changes>: methods::LoadQuery<crate::AsyncPgConnection, Output>,
    Changes::Table: Send,
    Changes::WhereClause: Send,
    Changes::Changeset: Send,
    <Changes::Table as Table>::AllColumns: ValidGrouping<()>,
    <<Changes::Table as Table>::AllColumns as ValidGrouping<()>>::IsAggregate:
        MixedAggregates<is_aggregate::No, Output = is_aggregate::No>,
    <Changes::Table as QuerySource>::FromClause: Send,
{
    async fn update_and_fetch(&mut self, changeset: Changes) -> QueryResult<Output>
    where
        Changes: 'async_trait,
    {
        diesel::update(changeset)
            .set(changeset)
            .get_result(self)
            .await
    }
}
