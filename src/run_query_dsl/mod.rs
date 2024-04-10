use crate::AsyncConnection;
use diesel::associations::HasTable;
use diesel::query_builder::IntoUpdateTarget;
use diesel::result::QueryResult;
use diesel::AsChangeset;
use futures_util::{future, stream, FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use std::pin::Pin;

/// The traits used by `QueryDsl`.
///
/// Each trait in this module represents exactly one method from [`RunQueryDsl`].
/// Apps should general rely on [`RunQueryDsl`] directly, rather than these traits.
/// However, generic code may need to include a where clause that references
/// these traits.
pub mod methods {
    use super::*;
    use diesel::backend::Backend;
    use diesel::deserialize::FromSqlRow;
    use diesel::expression::QueryMetadata;
    use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
    use diesel::query_dsl::CompatibleType;
    use futures_util::{Future, Stream, TryFutureExt};

    /// The `execute` method
    ///
    /// This trait should not be relied on directly by most apps. Its behavior is
    /// provided by [`RunQueryDsl`]. However, you may need a where clause on this trait
    /// to call `execute` from generic code.
    ///
    /// [`RunQueryDsl`]: super::RunQueryDsl
    pub trait ExecuteDsl<Conn, DB = <Conn as AsyncConnection>::Backend>
    where
        Conn: AsyncConnection<Backend = DB>,
        DB: Backend,
    {
        /// Execute this command
        fn execute<'conn, 'query>(
            query: Self,
            conn: &'conn mut Conn,
        ) -> Conn::ExecuteFuture<'conn, 'query>
        where
            Self: 'query;
    }

    impl<Conn, DB, T> ExecuteDsl<Conn, DB> for T
    where
        Conn: AsyncConnection<Backend = DB>,
        DB: Backend,
        T: QueryFragment<DB> + QueryId + Send,
    {
        fn execute<'conn, 'query>(
            query: Self,
            conn: &'conn mut Conn,
        ) -> Conn::ExecuteFuture<'conn, 'query>
        where
            Self: 'query,
        {
            conn.execute_returning_count(query)
        }
    }

    /// The `load` method
    ///
    /// This trait should not be relied on directly by most apps. Its behavior is
    /// provided by [`RunQueryDsl`]. However, you may need a where clause on this trait
    /// to call `load` from generic code.
    ///
    /// [`RunQueryDsl`]: super::RunQueryDsl
    pub trait LoadQuery<'query, Conn: AsyncConnection, U> {
        /// The future returned by [`LoadQuery::internal_load`]
        type LoadFuture<'conn>: Future<Output = QueryResult<Self::Stream<'conn>>> + Send
        where
            Conn: 'conn;
        /// The inner stream returned by [`LoadQuery::internal_load`]
        type Stream<'conn>: Stream<Item = QueryResult<U>> + Send
        where
            Conn: 'conn;

        /// Load this query
        fn internal_load(self, conn: &mut Conn) -> Self::LoadFuture<'_>;
    }

    impl<'query, Conn, DB, T, U, ST> LoadQuery<'query, Conn, U> for T
    where
        Conn: AsyncConnection<Backend = DB>,
        U: Send,
        DB: Backend + 'static,
        T: AsQuery + Send + 'query,
        T::Query: QueryFragment<DB> + QueryId + Send + 'query,
        T::SqlType: CompatibleType<U, DB, SqlType = ST>,
        U: FromSqlRow<ST, DB> + Send + 'static,
        DB: QueryMetadata<T::SqlType>,
        ST: 'static,
    {
        type LoadFuture<'conn> = future::MapOk<
            Conn::LoadFuture<'conn, 'query>,
            fn(Conn::Stream<'conn, 'query>) -> Self::Stream<'conn>,
        > where Conn: 'conn;

        type Stream<'conn> = stream::Map<
            Conn::Stream<'conn, 'query>,
            fn(
                QueryResult<Conn::Row<'conn, 'query>>,
            ) -> QueryResult<U>,
        >where  Conn: 'conn;

        fn internal_load(self, conn: &mut Conn) -> Self::LoadFuture<'_> {
            conn.load(self)
                .map_ok(map_result_stream_future::<U, _, _, DB, ST>)
        }
    }

    #[allow(clippy::type_complexity)]
    fn map_result_stream_future<'s, 'a, U, S, R, DB, ST>(
        stream: S,
    ) -> stream::Map<S, fn(QueryResult<R>) -> QueryResult<U>>
    where
        S: Stream<Item = QueryResult<R>> + Send + 's,
        R: diesel::row::Row<'a, DB> + 's,
        DB: Backend + 'static,
        U: FromSqlRow<ST, DB> + 'static,
        ST: 'static,
    {
        stream.map(map_row_helper::<_, DB, U, ST>)
    }

    fn map_row_helper<'a, R, DB, U, ST>(row: QueryResult<R>) -> QueryResult<U>
    where
        U: FromSqlRow<ST, DB>,
        R: diesel::row::Row<'a, DB>,
        DB: Backend,
    {
        U::build_from_row(&row?).map_err(diesel::result::Error::DeserializationError)
    }
}

/// The return types produced by the various [`RunQueryDsl`] methods
///
// We cannot box these types as this would require specifying a lifetime,
// but concrete connection implementations might want to have control
// about that so that they can support multiple simultaneous queries on
// the same connection
#[allow(type_alias_bounds)] // we need these bounds otherwise we cannot use GAT's
pub mod return_futures {
    use super::methods::LoadQuery;
    use diesel::QueryResult;
    use futures_util::{future, stream};
    use std::pin::Pin;

    /// The future returned by [`RunQueryDsl::load`](super::RunQueryDsl::load)
    /// and [`RunQueryDsl::get_results`](super::RunQueryDsl::get_results)
    ///
    /// This is essentially `impl Future<Output = QueryResult<Vec<U>>>`
    pub type LoadFuture<'conn, 'query, Q: LoadQuery<'query, Conn, U>, Conn, U> = future::AndThen<
        Q::LoadFuture<'conn>,
        stream::TryCollect<Q::Stream<'conn>, Vec<U>>,
        fn(Q::Stream<'conn>) -> stream::TryCollect<Q::Stream<'conn>, Vec<U>>,
    >;

    /// The future returned by [`RunQueryDsl::get_result`](super::RunQueryDsl::get_result)
    ///
    /// This is essentially `impl Future<Output = QueryResult<U>>`
    pub type GetResult<'conn, 'query, Q: LoadQuery<'query, Conn, U>, Conn, U> = future::AndThen<
        Q::LoadFuture<'conn>,
        future::Map<
            stream::StreamFuture<Pin<Box<Q::Stream<'conn>>>>,
            fn((Option<QueryResult<U>>, Pin<Box<Q::Stream<'conn>>>)) -> QueryResult<U>,
        >,
        fn(
            Q::Stream<'conn>,
        ) -> future::Map<
            stream::StreamFuture<Pin<Box<Q::Stream<'conn>>>>,
            fn((Option<QueryResult<U>>, Pin<Box<Q::Stream<'conn>>>)) -> QueryResult<U>,
        >,
    >;
}

/// Methods used to execute queries.
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
    /// use diesel_async::RunQueryDsl;
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
    /// let inserted_rows = insert_into(users)
    ///     .values(name.eq("Ruby"))
    ///     .execute(connection)
    ///     .await?;
    /// assert_eq!(1, inserted_rows);
    ///
    /// # #[cfg(not(feature = "sqlite"))]
    /// let inserted_rows = insert_into(users)
    ///     .values(&vec![name.eq("Jim"), name.eq("James")])
    ///     .execute(connection)
    ///     .await?;
    /// # #[cfg(not(feature = "sqlite"))]
    /// assert_eq!(2, inserted_rows);
    /// #     Ok(())
    /// # }
    /// ```
    fn execute<'conn, 'query>(self, conn: &'conn mut Conn) -> Conn::ExecuteFuture<'conn, 'query>
    where
        Conn: AsyncConnection + Send,
        Self: methods::ExecuteDsl<Conn> + 'query,
    {
        methods::ExecuteDsl::execute(self, conn)
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
    /// use diesel_async::{RunQueryDsl, AsyncConnection};
    ///
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
    /// use diesel_async::RunQueryDsl;
    ///
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
    /// use diesel_async::RunQueryDsl;
    ///
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
    fn load<'query, 'conn, U>(
        self,
        conn: &'conn mut Conn,
    ) -> return_futures::LoadFuture<'conn, 'query, Self, Conn, U>
    where
        U: Send,
        Conn: AsyncConnection,
        Self: methods::LoadQuery<'query, Conn, U> + 'query,
    {
        fn collect_result<U, S>(stream: S) -> stream::TryCollect<S, Vec<U>>
        where
            S: Stream<Item = QueryResult<U>>,
        {
            stream.try_collect()
        }
        self.internal_load(conn).and_then(collect_result::<U, _>)
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
    /// use diesel_async::RunQueryDsl;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await;
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::insert_into;
    /// #     use schema::users::dsl::*;
    /// #     use futures_util::stream::TryStreamExt;
    /// #     let connection = &mut establish_connection().await;
    /// let data = users.select(name)
    ///     .load_stream::<String>(connection)
    ///     .await?
    ///     .try_fold(Vec::new(), |mut acc, item| {
    ///          acc.push(item);
    ///          futures_util::future::ready(Ok(acc))
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
    /// use diesel_async::RunQueryDsl;
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test().await;
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use diesel::insert_into;
    /// #     use schema::users::dsl::*;
    /// #     use futures_util::stream::TryStreamExt;
    /// #     let connection = &mut establish_connection().await;
    /// let data = users
    ///     .load_stream::<(i32, String)>(connection)
    ///     .await?
    ///     .try_fold(Vec::new(), |mut acc, item| {
    ///          acc.push(item);
    ///          futures_util::future::ready(Ok(acc))
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
    /// use diesel_async::RunQueryDsl;
    ///
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
    /// #     use futures_util::stream::TryStreamExt;
    /// #     let connection = &mut establish_connection().await;
    /// let data = users
    ///     .load_stream::<User>(connection)
    ///     .await?
    ///     .try_fold(Vec::new(), |mut acc, item| {
    ///          acc.push(item);
    ///          futures_util::future::ready(Ok(acc))
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
    fn load_stream<'conn, 'query, U>(self, conn: &'conn mut Conn) -> Self::LoadFuture<'conn>
    where
        Conn: AsyncConnection,
        U: 'conn,
        Self: methods::LoadQuery<'query, Conn, U> + 'query,
    {
        self.internal_load(conn)
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
    /// use diesel_async::RunQueryDsl;
    ///
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
    fn get_result<'query, 'conn, U>(
        self,
        conn: &'conn mut Conn,
    ) -> return_futures::GetResult<'conn, 'query, Self, Conn, U>
    where
        U: Send + 'conn,
        Conn: AsyncConnection,
        Self: methods::LoadQuery<'query, Conn, U> + 'query,
    {
        #[allow(clippy::type_complexity)]
        fn get_next_stream_element<S, U>(
            stream: S,
        ) -> future::Map<
            stream::StreamFuture<Pin<Box<S>>>,
            fn((Option<QueryResult<U>>, Pin<Box<S>>)) -> QueryResult<U>,
        >
        where
            S: Stream<Item = QueryResult<U>>,
        {
            fn map_option_to_result<U, S>(
                (o, _): (Option<QueryResult<U>>, Pin<Box<S>>),
            ) -> QueryResult<U> {
                match o {
                    Some(s) => s,
                    None => Err(diesel::result::Error::NotFound),
                }
            }

            Box::pin(stream).into_future().map(map_option_to_result)
        }

        self.load_stream(conn).and_then(get_next_stream_element)
    }

    /// Runs the command, returning an `Vec` with the affected rows.
    ///
    /// This method is an alias for [`load`], but with a name that makes more
    /// sense for insert, update, and delete statements.
    ///
    /// [`load`]: crate::run_query_dsl::RunQueryDsl::load()
    fn get_results<'query, 'conn, U>(
        self,
        conn: &'conn mut Conn,
    ) -> return_futures::LoadFuture<'conn, 'query, Self, Conn, U>
    where
        U: Send,
        Conn: AsyncConnection,
        Self: methods::LoadQuery<'query, Conn, U> + 'query,
    {
        self.load(conn)
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
    /// use diesel_async::RunQueryDsl;
    ///
    /// #
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// #     run_test();
    /// # }
    /// #
    /// # async fn run_test() -> QueryResult<()> {
    /// #     use schema::users::dsl::*;
    /// #     let connection = &mut establish_connection().await;
    /// for n in &["Sean", "Pascal"] {
    ///     diesel::insert_into(users)
    ///         .values(name.eq(n))
    ///         .execute(connection)
    ///         .await?;
    /// }
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
    fn first<'query, 'conn, U>(
        self,
        conn: &'conn mut Conn,
    ) -> return_futures::GetResult<'conn, 'query, diesel::dsl::Limit<Self>, Conn, U>
    where
        U: Send + 'conn,
        Conn: AsyncConnection,
        Self: diesel::query_dsl::methods::LimitDsl,
        diesel::dsl::Limit<Self>: methods::LoadQuery<'query, Conn, U> + Send + 'query,
    {
        diesel::query_dsl::methods::LimitDsl::limit(self, 1).get_result(conn)
    }
}

impl<T, Conn> RunQueryDsl<Conn> for T {}

/// Sugar for types which implement both `AsChangeset` and `Identifiable`
///
/// On backends which support the `RETURNING` keyword,
/// `foo.save_changes(&conn)` is equivalent to
/// `update(&foo).set(&foo).get_result(&conn)`.
/// On other backends, two queries will be executed.
///
/// # Example
///
/// ```rust
/// # include!("../doctest_setup.rs");
/// # use schema::animals;
/// #
/// use diesel_async::{SaveChangesDsl, AsyncConnection};
///
/// #[derive(Queryable, Debug, PartialEq)]
/// struct Animal {
///    id: i32,
///    species: String,
///    legs: i32,
///    name: Option<String>,
/// }
///
/// #[derive(AsChangeset, Identifiable)]
/// #[diesel(table_name = animals)]
/// struct AnimalForm<'a> {
///     id: i32,
///     name: &'a str,
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// #     run_test().await.unwrap();
/// # }
/// #
/// # async fn run_test() -> QueryResult<()> {
/// #     use self::animals::dsl::*;
/// #     let connection = &mut establish_connection().await;
/// let form = AnimalForm { id: 2, name: "Super scary" };
/// # #[cfg(not(feature = "sqlite"))]
/// let changed_animal = form.save_changes(connection).await?;
/// let expected_animal = Animal {
///     id: 2,
///     species: String::from("spider"),
///     legs: 8,
///     name: Some(String::from("Super scary")),
/// };
/// # #[cfg(not(feature = "sqlite"))]
/// assert_eq!(expected_animal, changed_animal);
/// #     Ok(())
/// # }
/// ```
#[async_trait::async_trait]
pub trait SaveChangesDsl<Conn> {
    /// See the trait documentation
    async fn save_changes<T>(self, connection: &mut Conn) -> QueryResult<T>
    where
        Self: Sized + diesel::prelude::Identifiable,
        Conn: UpdateAndFetchResults<Self, T>,
    {
        connection.update_and_fetch(self).await
    }
}

impl<T, Conn> SaveChangesDsl<Conn> for T where
    T: Copy + AsChangeset<Target = <T as HasTable>::Table> + IntoUpdateTarget
{
}

/// A trait defining how to update a record and fetch the updated entry
/// on a certain backend.
///
/// The only case where it is required to work with this trait is while
/// implementing a new connection type.
/// Otherwise use [`SaveChangesDsl`]
///
/// For implementing this trait for a custom backend:
/// * The `Changes` generic parameter represents the changeset that should be stored
/// * The `Output` generic parameter represents the type of the response.
#[async_trait::async_trait]
pub trait UpdateAndFetchResults<Changes, Output>: AsyncConnection
where
    Changes: diesel::prelude::Identifiable + HasTable,
{
    /// See the traits documentation.
    async fn update_and_fetch(&mut self, changeset: Changes) -> QueryResult<Output>
    where
        Changes: 'async_trait;
}

#[cfg(feature = "mysql")]
#[async_trait::async_trait]
impl<'b, Changes, Output> UpdateAndFetchResults<Changes, Output> for crate::AsyncMysqlConnection
where
    Output: Send,
    Changes: Copy + diesel::Identifiable + Send,
    Changes: AsChangeset<Target = <Changes as HasTable>::Table> + IntoUpdateTarget,
    Changes::Table: diesel::query_dsl::methods::FindDsl<Changes::Id> + Send,
    Changes::WhereClause: Send,
    Changes::Changeset: Send,
    Changes::Id: Send,
    diesel::dsl::Update<Changes, Changes>: methods::ExecuteDsl<crate::AsyncMysqlConnection>,
    diesel::dsl::Find<Changes::Table, Changes::Id>:
        methods::LoadQuery<'b, crate::AsyncMysqlConnection, Output> + Send + 'b,
    <Changes::Table as diesel::Table>::AllColumns: diesel::expression::ValidGrouping<()>,
    <<Changes::Table as diesel::Table>::AllColumns as diesel::expression::ValidGrouping<()>>::IsAggregate: diesel::expression::MixedAggregates<
        diesel::expression::is_aggregate::No,
        Output = diesel::expression::is_aggregate::No,
    >,
    <Changes::Table as diesel::query_source::QuerySource>::FromClause: Send,
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
impl<'b, Changes, Output, Tab, V> UpdateAndFetchResults<Changes, Output>
    for crate::AsyncPgConnection
where
    Output: Send,
    Changes:
        Copy + AsChangeset<Target = Tab> + Send + diesel::associations::Identifiable<Table = Tab>,
    Tab: diesel::Table + diesel::query_dsl::methods::FindDsl<Changes::Id> + 'b,
    diesel::dsl::Find<Tab, Changes::Id>: IntoUpdateTarget<Table = Tab, WhereClause = V>,
    diesel::query_builder::UpdateStatement<Tab, V, Changes::Changeset>:
        diesel::query_builder::AsQuery,
    diesel::dsl::Update<Changes, Changes>: methods::LoadQuery<'b, Self, Output>,
    V: Send + 'b,
    Changes::Changeset: Send + 'b,
    Tab::FromClause: Send,
{
    async fn update_and_fetch(&mut self, changeset: Changes) -> QueryResult<Output>
    where
        Changes: 'async_trait,
        Changes::Changeset: 'async_trait,
    {
        diesel::update(changeset)
            .set(changeset)
            .get_result(self)
            .await
    }
}
