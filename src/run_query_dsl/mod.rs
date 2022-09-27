use std::pin::Pin;

use crate::AsyncConnection;
use diesel::associations::HasTable;
use diesel::query_builder::IntoUpdateTarget;
use diesel::result::QueryResult;
use diesel::AsChangeset;
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};

/// The traits used by `QueryDsl`.
///
/// Each trait in this module represents exactly one method from [`RunQueryDsl`].
/// Apps should general rely on [`RunQueryDsl`] directly, rather than these traits.
/// However, generic code may need to include a where clause that references
/// these traits.
pub mod methods {
    use super::*;
    use crate::AsyncConnectionGatWorkaround;
    use diesel::backend::Backend;
    use diesel::deserialize::FromSqlRow;
    use diesel::expression::QueryMetadata;
    use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
    use diesel::query_dsl::CompatibleType;
    use futures::{Future, Stream, TryFutureExt};

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
        ) -> <Conn as AsyncConnectionGatWorkaround<'conn, 'query, Conn::Backend>>::ExecuteFuture
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
        ) -> <Conn as AsyncConnectionGatWorkaround<'conn, 'query, Conn::Backend>>::ExecuteFuture
        where
            Self: 'query,
        {
            conn.execute_returning_count(query)
        }
    }

    /// This trait is a workaround to emulate GAT on stable rust
    ///
    /// It is used to specify the return type of [`LoadQuery::internal_load`]
    /// which may contain lifetimes
    pub trait LoadQueryGatWorkaround<'conn, 'query, Conn, U> {
        /// The future returned by [`LoadQuery::internal_load`]
        type LoadFuture: Future<Output = QueryResult<Self::Stream>> + Send;
        /// The inner stream returned by [`LoadQuery::internal_load`]
        type Stream: Stream<Item = QueryResult<U>> + Send;
    }

    /// The `load` method
    ///
    /// This trait should not be relied on directly by most apps. Its behavior is
    /// provided by [`RunQueryDsl`]. However, you may need a where clause on this trait
    /// to call `load` from generic code.
    ///
    /// [`RunQueryDsl`]: super::RunQueryDsl
    pub trait LoadQuery<'query, Conn: AsyncConnection, U>
    where
        for<'a> Self: LoadQueryGatWorkaround<'a, 'query, Conn, U>,
    {
        /// Load this query
        fn internal_load<'conn>(
            self,
            conn: &'conn mut Conn,
        ) -> <Self as LoadQueryGatWorkaround<'conn, 'query, Conn, U>>::LoadFuture;
    }

    impl<'conn, 'query, Conn, U, T, DB, ST> LoadQueryGatWorkaround<'conn, 'query, Conn, U> for T
    where
        Conn: AsyncConnection<Backend = DB>,
        U: Send,
        T: AsQuery + Send,
        T::SqlType: CompatibleType<U, DB, SqlType = ST>,
        U: FromSqlRow<ST, DB> + Send + 'static,
        DB: QueryMetadata<T::SqlType>,
    {
        type LoadFuture = futures::future::MapOk<
            <Conn as AsyncConnectionGatWorkaround<'conn, 'query, DB>>::LoadFuture,
            fn(<Conn as AsyncConnectionGatWorkaround<'conn, 'query, DB>>::Stream) -> Self::Stream,
        >;
        type Stream = futures::stream::Map<
            <Conn as AsyncConnectionGatWorkaround<'conn, 'query, DB>>::Stream,
            fn(
                QueryResult<<Conn as AsyncConnectionGatWorkaround<'conn, 'query, DB>>::Row>,
            ) -> QueryResult<U>,
        >;
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
        fn internal_load<'conn>(
            self,
            conn: &'conn mut Conn,
        ) -> <Self as LoadQueryGatWorkaround<'conn, 'query, Conn, U>>::LoadFuture {
            // this cast is required to make rustc happy
            // it seems to get otherwise confused about the type of
            // this function pointer
            let f = map_result_stream_future::<U, _, _, DB, ST> as _;
            conn.load(self).map_ok(f)
        }
    }

    #[allow(clippy::type_complexity)]
    fn map_result_stream_future<'s, 'a, U, S, R, DB, ST>(
        stream: S,
    ) -> futures::stream::Map<S, fn(QueryResult<R>) -> QueryResult<U>>
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

/// The return types produces by the various [`RunQueryDsl`] methods
///
// We cannot box these types as this would require specifying a lifetime,
// but concrete connection implementations might want to have control
// about that so that they can support multiple simultan queries on
// the same connection
pub mod return_futures {
    use super::methods;
    use crate::{AsyncConnection, AsyncConnectionGatWorkaround};
    use diesel::QueryResult;
    use std::pin::Pin;

    /// The future returned by [`RunQueryDsl::load`](super::RunQueryDsl::load)
    /// and [`RunQueryDsl::get_results`](super::RunQueryDsl::get_results)
    ///
    /// This is essentially `impl Future<Output = QueryResult<Vec<U>>>`
    pub type LoadFuture<'conn, 'query, Q, Conn, U> = futures::future::AndThen<
        <Q as methods::LoadQueryGatWorkaround<'conn, 'query, Conn, U>>::LoadFuture,
        futures::stream::TryCollect<
            <Q as methods::LoadQueryGatWorkaround<'conn, 'query, Conn, U>>::Stream,
            Vec<U>,
        >,
        fn(
            <Q as methods::LoadQueryGatWorkaround<'conn, 'query, Conn, U>>::Stream,
        ) -> futures::stream::TryCollect<
            <Q as methods::LoadQueryGatWorkaround<'conn, 'query, Conn, U>>::Stream,
            Vec<U>,
        >,
    >;

    /// The future returned by [`RunQueryDsl::get_result`](super::RunQueryDsl::get_result)
    ///
    /// This is essentially `impl Future<Output = QueryResult<U>>`
    pub type GetResult<'conn, 'query, Q, Conn, U> = futures::future::AndThen<
        <Q as methods::LoadQueryGatWorkaround<'conn, 'query, Conn, U>>::LoadFuture,
        futures::future::Map<
            futures::stream::StreamFuture<
                Pin<Box<<Q as methods::LoadQueryGatWorkaround<'conn, 'query, Conn, U>>::Stream>>,
            >,
            fn(
                (
                    Option<QueryResult<U>>,
                    Pin<
                        Box<<Q as methods::LoadQueryGatWorkaround<'conn, 'query, Conn, U>>::Stream>,
                    >,
                ),
            ) -> QueryResult<U>,
        >,
        fn(
            <Q as methods::LoadQueryGatWorkaround<'conn, 'query, Conn, U>>::Stream,
        ) -> futures::future::Map<
            futures::stream::StreamFuture<
                Pin<Box<<Q as methods::LoadQueryGatWorkaround<'conn, 'query, Conn, U>>::Stream>>,
            >,
            fn(
                (
                    Option<QueryResult<U>>,
                    Pin<
                        Box<<Q as methods::LoadQueryGatWorkaround<'conn, 'query, Conn, U>>::Stream>,
                    >,
                ),
            ) -> QueryResult<U>,
        >,
    >;

    /// The future returned by [`RunQueryDsl::execute`](super::RunQueryDsl::execute)
    ///
    /// This is essentially `impl Future<Output = QueryResult<usize>>`
    pub type Execute<'conn, 'query, Conn> = <Conn as AsyncConnectionGatWorkaround<
        'conn,
        'query,
        <Conn as AsyncConnection>::Backend,
    >>::ExecuteFuture;
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
    fn execute<'conn, 'query>(
        self,
        conn: &'conn mut Conn,
    ) -> return_futures::Execute<'conn, 'query, Conn>
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
    fn load<'query, 'conn, U>(
        self,
        conn: &'conn mut Conn,
    ) -> return_futures::LoadFuture<'conn, 'query, Self, Conn, U>
    where
        U: Send,
        Conn: AsyncConnection,
        Self: methods::LoadQuery<'query, Conn, U> + 'query,
    {
        fn collect_result<U, S>(stream: S) -> futures::stream::TryCollect<S, Vec<U>>
        where
            S: Stream<Item = QueryResult<U>>,
        {
            stream.try_collect()
        }
        let load_future = self.internal_load(conn);

        // this cast is required to make rustc happy
        // it seems to get otherwise confused about the type of
        // this function pointer

        let f = collect_result::<U, _> as _;
        load_future.and_then(f)
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
    fn load_stream<'conn, 'query, U>(
        self,
        conn: &'conn mut Conn,
    ) -> <Self as methods::LoadQueryGatWorkaround<'conn, 'query, Conn, U>>::LoadFuture
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
        ) -> futures::future::Map<
            futures::stream::StreamFuture<Pin<Box<S>>>,
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

            let stream = Box::pin(stream);
            let f = map_option_to_result as _;
            stream.into_future().map(f)
        }
        let f = get_next_stream_element as _;
        self.load_stream(conn).and_then(f)
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
/// let changed_animal = form.save_changes(connection).await?;
/// let expected_animal = Animal {
///     id: 2,
///     species: String::from("spider"),
///     legs: 8,
///     name: Some(String::from("Super scary")),
/// };
/// assert_eq!(expected_animal, changed_animal);
/// #     Ok(())
/// # }
/// ```
#[async_trait::async_trait]
pub trait SaveChangesDsl<Conn> {
    /// See the trait documentation
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
pub trait UpdateAndFetchResults<Changes, Output>: AsyncConnection {
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
impl<'b, Changes, Output> UpdateAndFetchResults<Changes, Output> for crate::AsyncPgConnection
where
    Output: Send,
    Changes: Copy + AsChangeset<Target = <Changes as HasTable>::Table> + IntoUpdateTarget + Send,
    diesel::dsl::Update<Changes, Changes>: methods::LoadQuery<'b, crate::AsyncPgConnection, Output>,
    Changes::Table: Send + 'b,
    Changes::WhereClause: Send + 'b,
    Changes::Changeset: Send + 'b,
    <Changes::Table as diesel::Table>::AllColumns: diesel::expression::ValidGrouping<()>,
    <<Changes::Table as diesel::Table>::AllColumns as diesel::expression::ValidGrouping<()>>::IsAggregate:
        diesel::expression::MixedAggregates<diesel::expression::is_aggregate::No, Output = diesel::expression::is_aggregate::No>,
    <Changes::Table as diesel::query_source::QuerySource>::FromClause: Send,
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
