use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::QueryResult;
use diesel_async::*;
use scoped_futures::ScopedFutureExt;
use std::fmt::Debug;

#[cfg(feature = "postgres")]
mod custom_types;
mod instrumentation;
#[cfg(any(feature = "bb8", feature = "deadpool", feature = "mobc"))]
mod pooling;
#[cfg(feature = "async-connection-wrapper")]
mod sync_wrapper;
mod type_check;

async fn transaction_test<C: AsyncConnection<Backend = TestBackend>>(
    conn: &mut C,
) -> QueryResult<()> {
    let res = conn
        .transaction::<i32, diesel::result::Error, _>(|conn| {
            async move {
                let users: Vec<User> = users::table.load(conn).await?;
                assert_eq!(&users[0].name, "John Doe");
                assert_eq!(&users[1].name, "Jane Doe");

                let user: Option<User> = users::table.find(42).first(conn).await.optional()?;
                assert_eq!(user, None::<User>);

                let res = conn
                    .transaction::<_, diesel::result::Error, _>(|conn| {
                        async move {
                            diesel::insert_into(users::table)
                                .values(users::name.eq("Dave"))
                                .execute(conn)
                                .await?;
                            let count = users::table.count().get_result::<i64>(conn).await?;
                            assert_eq!(count, 3);
                            Ok(())
                        }
                        .scope_boxed()
                    })
                    .await;
                assert!(res.is_ok());
                let count = users::table.count().get_result::<i64>(conn).await?;
                assert_eq!(count, 3);

                let res = diesel::insert_into(users::table)
                    .values(users::name.eq("Eve"))
                    .execute(conn)
                    .await?;

                assert_eq!(res, 1, "Insert in transaction returned wrong result");
                let count = users::table.count().get_result::<i64>(conn).await?;
                assert_eq!(count, 4);

                Err(diesel::result::Error::RollbackTransaction)
            }
            .scope_boxed()
        })
        .await;
    assert_eq!(
        res,
        Err(diesel::result::Error::RollbackTransaction),
        "Failed to rollback transaction"
    );

    let count = users::table.count().get_result::<i64>(conn).await?;
    assert_eq!(count, 2, "user got committed, but transaction rolled back");

    Ok(())
}

diesel::table! {
    users {
        id -> Integer,
        name -> Text,
    }
}

#[derive(
    diesel::Queryable,
    diesel::Selectable,
    Debug,
    PartialEq,
    diesel::AsChangeset,
    diesel::Identifiable,
)]
struct User {
    id: i32,
    name: String,
}

#[cfg(feature = "mysql")]
type TestConnection = AsyncMysqlConnection;
#[cfg(feature = "postgres")]
type TestConnection = AsyncPgConnection;
#[cfg(feature = "sqlite")]
type TestConnection =
    sync_connection_wrapper::SyncConnectionWrapper<diesel::sqlite::SqliteConnection>;

#[allow(dead_code)]
type TestBackend = <TestConnection as AsyncConnection>::Backend;

#[tokio::test]
async fn test_basic_insert_and_load() -> QueryResult<()> {
    let conn = &mut connection().await;
    // Insertion split into 2 since Sqlite batch insert isn't supported for diesel_async yet
    let res = diesel::insert_into(users::table)
        .values(users::name.eq("John Doe"))
        .execute(conn)
        .await;
    assert_eq!(res, Ok(1), "User count does not match");
    let res = diesel::insert_into(users::table)
        .values(users::name.eq("Jane Doe"))
        .execute(conn)
        .await;
    assert_eq!(res, Ok(1), "User count does not match");
    let users = users::table.load::<User>(conn).await?;
    assert_eq!(&users[0].name, "John Doe", "User name [0] does not match");
    assert_eq!(&users[1].name, "Jane Doe", "User name [1] does not match");

    transaction_test(conn).await?;

    Ok(())
}

#[cfg(feature = "postgres")]
diesel::define_sql_function!(fn pg_sleep(interval: diesel::sql_types::Double));

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_cancel_token() {
    use std::time::Duration;

    use diesel::result::{DatabaseErrorKind, Error};

    let conn = &mut connection().await;

    let token = conn.cancel_token();

    // execute a query that runs for a long time
    let long_running_query = diesel::select(pg_sleep(5.0)).execute(conn);

    // execute the query elsewhere...
    let task = tokio::spawn(async move {
        long_running_query
            .await
            .expect_err("query should have been canceled.")
    });

    // let the task above have some time to actually start...
    tokio::time::sleep(Duration::from_millis(500)).await;

    // invoke the cancellation token.
    token.cancel_query(tokio_postgres::NoTls).await.unwrap();

    // make sure the query task resulted in a cancellation error
    let err = task.await.unwrap();
    match err {
        Error::DatabaseError(DatabaseErrorKind::Unknown, v)
            if v.message() == "canceling statement due to user request" => {}
        _ => panic!("unexpected error: {:?}", err),
    }
}

#[cfg(feature = "postgres")]
async fn setup(connection: &mut TestConnection) {
    diesel::sql_query(
        "CREATE TEMPORARY TABLE users (
                id SERIAL PRIMARY KEY,
                name VARCHAR NOT NULL
            )",
    )
    .execute(connection)
    .await
    .unwrap();
}

#[cfg(feature = "sqlite")]
async fn setup(connection: &mut TestConnection) {
    diesel::sql_query(
        "CREATE TEMPORARY TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            )",
    )
    .execute(connection)
    .await
    .unwrap();
}

#[cfg(feature = "mysql")]
async fn setup(connection: &mut TestConnection) {
    diesel::sql_query(
        "CREATE TEMPORARY TABLE users (
                id INTEGER PRIMARY KEY AUTO_INCREMENT,
                name TEXT NOT NULL
            ) CHARACTER SET utf8mb4",
    )
    .execute(connection)
    .await
    .unwrap();
}

async fn connection() -> TestConnection {
    let mut conn = connection_without_transaction().await;
    if cfg!(feature = "postgres") {
        // postgres allows to modify the schema inside of a transaction
        conn.begin_test_transaction().await.unwrap();
    }
    setup(&mut conn).await;
    if cfg!(feature = "mysql") || cfg!(feature = "sqlite") {
        // mysql does not allow this and does even automatically close
        // any open transaction. As of this we open a transaction **after**
        // we setup the schema
        conn.begin_test_transaction().await.unwrap();
    }
    conn
}

async fn connection_without_transaction() -> TestConnection {
    let db_url = std::env::var("DATABASE_URL").unwrap();
    TestConnection::establish(&db_url).await.unwrap()
}
