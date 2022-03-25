use diesel::prelude::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel::QueryResult;
use diesel_async::*;
use std::fmt::Debug;
use std::pin::Pin;

#[cfg(feature = "postgres")]
mod custom_types;
mod type_check;

async fn transaction_test(conn: &mut TestConnection) -> QueryResult<()> {
    let res = conn
        .transaction::<_, i32, diesel::result::Error>(|conn| {
            Box::pin(async move {
                let users: Vec<User> = users::table.load(conn).await?;
                assert_eq!(&users[0].name, "John Doe");
                assert_eq!(&users[1].name, "Jane Doe");

                let user: Option<User> = users::table.find(42).first(conn).await.optional()?;
                assert_eq!(user, None::<User>);

                let res = diesel::insert_into(users::table)
                    .values(users::name.eq("Eve"))
                    .execute(conn)
                    .await?;

                assert_eq!(res, 1, "Insert in transaction returned wrong result");

                Err(diesel::result::Error::RollbackTransaction)
            }) as Pin<Box<_>>
        })
        .await;
    assert_eq!(
        res,
        Err(diesel::result::Error::RollbackTransaction),
        "Failed to rollback transaction"
    );

    let count = users::table.count().get_result::<i64>(conn).await?;
    assert_eq!(count, 2, "user got commited, but transaction rolled back");

    Ok(())
}

diesel::table! {
    users {
        id -> Integer,
        name -> Text,
    }
}

#[derive(diesel::Queryable, diesel::Selectable, Debug, PartialEq)]
struct User {
    id: i32,
    name: String,
}

#[cfg(feature = "mysql")]
type TestConnection = AsyncMysqlConnection;
#[cfg(feature = "postgres")]
type TestConnection = AsyncPgConnection;

#[tokio::test]
async fn test_basic_insert_and_load() -> QueryResult<()> {
    let conn = &mut connection().await;
    let res = diesel::insert_into(users::table)
        .values([users::name.eq("John Doe"), users::name.eq("Jane Doe")])
        .execute(conn)
        .await;
    assert_eq!(res, Ok(2), "User count does not match");
    let users = users::table.load::<User>(conn).await?;
    assert_eq!(&users[0].name, "John Doe", "User name [0] does not match");
    assert_eq!(&users[1].name, "Jane Doe", "User name [1] does not match");

    transaction_test(conn).await?;

    Ok(())
}

#[cfg(feature = "mysql")]
async fn setup(connection: &mut TestConnection) {
    diesel::sql_query(
        "CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTO_INCREMENT,
                name TEXT NOT NULL
            ) CHARACTER SET utf8mb4",
    )
    .execute(connection)
    .await
    .unwrap();
}

#[cfg(feature = "postgres")]
async fn setup(connection: &mut TestConnection) {
    diesel::sql_query(
        "CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR NOT NULL
            )",
    )
    .execute(connection)
    .await
    .unwrap();
}

async fn connection() -> TestConnection {
    let db_url = std::env::var("DATABASE_URL").unwrap();
    let mut conn = TestConnection::establish(&db_url).await.unwrap();
    conn.begin_test_transaction().await.unwrap();
    setup(&mut conn);
    conn
}
