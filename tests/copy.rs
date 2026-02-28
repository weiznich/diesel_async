use diesel::dsl::copy_from;
use diesel::prelude::*;
use diesel_async::pg::copy::AsyncExecuteCopyFromDsl;
use diesel_async::RunQueryDsl;

use super::{connection, users};

#[derive(Insertable)]
#[diesel(table_name = users)]
#[diesel(treat_none_as_default_value = false)]
struct NewUser<'a> {
    name: &'a str,
}

#[tokio::test]
async fn copy_from_raw_data() {
    let mut conn = connection().await;

    let result = AsyncExecuteCopyFromDsl::execute(
        copy_from(users::table).from_raw_data((users::name,), |writer: &mut dyn std::io::Write| {
            writeln!(writer, "Alice").unwrap();
            writeln!(writer, "Bob").unwrap();
            Ok::<(), diesel::result::Error>(())
        }),
        &mut conn,
    )
    .await;

    assert_eq!(result.unwrap(), 2, "should have copied 2 rows");

    let names: Vec<String> = users::table
        .select(users::name)
        .order(users::name.asc())
        .load(&mut conn)
        .await
        .unwrap();

    assert_eq!(names, vec!["Alice", "Bob"]);
}

#[tokio::test]
async fn copy_from_insertable() {
    let mut conn = connection().await;

    let new_users = vec![
        NewUser { name: "Charlie" },
        NewUser { name: "Diana" },
        NewUser { name: "Eve" },
    ];

    let result = AsyncExecuteCopyFromDsl::execute(
        copy_from(users::table).from_insertable(new_users),
        &mut conn,
    )
    .await;

    assert_eq!(result.unwrap(), 3, "should have copied 3 rows");

    let names: Vec<String> = users::table
        .select(users::name)
        .order(users::name.asc())
        .load(&mut conn)
        .await
        .unwrap();

    assert_eq!(names, vec!["Charlie", "Diana", "Eve"]);
}
