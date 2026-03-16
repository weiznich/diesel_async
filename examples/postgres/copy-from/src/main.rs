use diesel::prelude::*;
use diesel_async::pg::copy::AsyncExecuteCopyFromDsl;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use std::io::Write;

diesel::table! {
    users {
        id -> Integer,
        name -> Text,
    }
}

#[derive(Insertable)]
#[diesel(table_name = users)]
struct NewUser<'a> {
    name: &'a str,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_url = std::env::var("DATABASE_URL").expect("Env var `DATABASE_URL` not set");
    let mut conn = AsyncPgConnection::establish(&db_url).await?;

    // Example 1: Copy from raw data using a callback
    println!("Example 1: Copy from raw data");
    let rows_copied = diesel::dsl::copy_from(users::table)
        .from_raw_data((users::name,), |writer: &mut dyn Write| {
            writeln!(writer, "Alice")?;
            writeln!(writer, "Bob")?;
            Ok::<(), std::io::Error>(())
        })
        .execute(&mut conn)
        .await?;
    println!("Copied {rows_copied} rows from raw data.");

    // Example 2: Copy from insertable data
    println!("\nExample 2: Copy from insertable data");
    let data = vec![
        NewUser { name: "Charlie" },
        NewUser { name: "David" },
        NewUser { name: "Eve" },
    ];
    let rows_copied = diesel::dsl::copy_from(users::table)
        .from_insertable(data)
        .execute(&mut conn)
        .await?;
    println!("Copied {rows_copied} rows from insertable data.");

    Ok(())
}
