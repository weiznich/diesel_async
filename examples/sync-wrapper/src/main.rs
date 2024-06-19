use diesel::prelude::*;
use diesel::sqlite::{Sqlite, SqliteConnection};
use diesel_async::async_connection_wrapper::AsyncConnectionWrapper;
use diesel_async::sync_connection_wrapper::SyncConnectionWrapper;
use diesel_async::{AsyncConnection, RunQueryDsl, SimpleAsyncConnection};
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use futures_util::FutureExt;

// ordinary diesel model setup

table! {
    users {
        id -> Integer,
        name -> Text,
    }
}

#[allow(dead_code)]
#[derive(Debug, Queryable, QueryableByName, Selectable)]
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

async fn transaction(
    async_conn: &mut SyncConnectionWrapper<InnerConnection>,
    old_name: &str,
    new_name: &str,
) -> Result<Vec<User>, diesel::result::Error> {
    async_conn
        .transaction::<Vec<User>, diesel::result::Error, _>(|c| {
            Box::pin(async {
                if old_name.is_empty() {
                    Ok(Vec::new())
                } else {
                    diesel::sql_query(
                        r#"
                    update
                        users
                    set
                        name = ?2
                    where
                        name == ?1
                    returning *
                "#,
                    )
                    .bind::<diesel::sql_types::Text, _>(old_name)
                    .bind::<diesel::sql_types::Text, _>(new_name)
                    .load(c)
                    .await
                }
            })
        })
        .await
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

    // let changed = transaction(&mut sync_wrapper, "iLuke", "JustLuke").await?;
    // println!("Changed {changed:?}");

    // create an async connection for the migrations
    let mut conn_a: SyncConnectionWrapper<InnerConnection> = establish(&db_url).await?;
    let mut conn_b: SyncConnectionWrapper<InnerConnection> = establish(&db_url).await?;

    tokio::spawn(async move {
        loop {
            let changed = transaction(&mut conn_a, "iLuke", "JustLuke").await;
            println!("Changed {changed:?}");
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    });

    tokio::spawn(async move {
        loop {
            let changed = transaction(&mut conn_b, "JustLuke", "iLuke").await;
            println!("Changed {changed:?}");
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    });

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }

    Ok(())
}
