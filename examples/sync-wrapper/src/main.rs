use diesel::prelude::*;
use diesel::sqlite::{Sqlite, SqliteConnection};
use diesel_async::async_connection_wrapper::AsyncConnectionWrapper;
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, RunQueryDsl, SimpleAsyncConnection};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};

// ordinary diesel model setup

table! {
    users {
        id -> Integer,
        name -> Text,
    }
}

#[allow(dead_code)]
#[derive(Debug, Queryable, Selectable)]
#[diesel(table_name = users)]
struct User {
    id: i32,
    name: String,
}

const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

type InnerConnection = SqliteConnection;

type InnerDB = Sqlite;

async fn establish(db_url: &str) -> ConnectionResult<SyncConnectionWrapper<InnerConnection>> {
    // It is necessary to specify the specific inner connection type because of inference issues
    SyncConnectionWrapper::<SqliteConnection>::establish(db_url).await
}

async fn run_migrations<A>(async_connection: A) -> Result<(), Box<dyn std::error::Error>>
where
    A: AsyncConnection<Backend = InnerDB> + 'static,
{
    let mut async_wrapper: AsyncConnectionWrapper<A> =
        AsyncConnectionWrapper::from(async_connection);

    tokio::task::spawn_blocking(move || {
        async_wrapper.run_pending_migrations(MIGRATIONS).unwrap();
    })
    .await
    .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_url = std::env::var("DATABASE_URL").expect("Env var `DATABASE_URL` not set");

    // create an async connection for the migrations
    let sync_wrapper: SyncConnectionWrapper<InnerConnection> = establish(&db_url).await?;
    run_migrations(sync_wrapper).await?;

    let mut sync_wrapper: SyncConnectionWrapper<InnerConnection> = establish(&db_url).await?;

    sync_wrapper.batch_execute("DELETE FROM users").await?;

    sync_wrapper
        .batch_execute("INSERT INTO users(id, name) VALUES (3, 'toto')")
        .await?;

    let data: Vec<User> = users::table
        .select(User::as_select())
        .load(&mut sync_wrapper)
        .await?;
    println!("{data:?}");

    diesel::delete(users::table)
        .execute(&mut sync_wrapper)
        .await?;

    diesel::insert_into(users::table)
        .values((users::id.eq(1), users::name.eq("iLuke")))
        .execute(&mut sync_wrapper)
        .await?;

    let data: Vec<User> = users::table
        .filter(users::id.gt(0))
        .or_filter(users::name.like("%Luke"))
        .select(User::as_select())
        .load(&mut sync_wrapper)
        .await?;
    println!("{data:?}");

    Ok(())
}
