use diesel::migration::Migration;
use diesel::prelude::*;
use diesel_async::async_connection_wrapper::AsyncConnectionWrapper;

#[test]
fn test_sync_wrapper() {
    let db_url = std::env::var("DATABASE_URL").unwrap();
    let mut conn = AsyncConnectionWrapper::<crate::TestConnection>::establish(&db_url).unwrap();

    let res =
        diesel::select(1.into_sql::<diesel::sql_types::Integer>()).get_result::<i32>(&mut conn);
    assert_eq!(Ok(1), res);
}

#[tokio::test]
async fn test_sync_wrapper_under_runtime() {
    let db_url = std::env::var("DATABASE_URL").unwrap();
    tokio::task::spawn_blocking(move || {
        let mut conn = AsyncConnectionWrapper::<crate::TestConnection>::establish(&db_url).unwrap();

        let res =
            diesel::select(1.into_sql::<diesel::sql_types::Integer>()).get_result::<i32>(&mut conn);
        assert_eq!(Ok(1), res);
    })
    .await
    .unwrap();
}

#[test]
fn check_run_migration() {
    use diesel_migrations::MigrationHarness;

    let db_url = std::env::var("DATABASE_URL").unwrap();
    let migrations: Vec<Box<dyn Migration<crate::TestBackend>>> = Vec::new();
    let mut conn = AsyncConnectionWrapper::<crate::TestConnection>::establish(&db_url).unwrap();

    // just use `run_migrations` here because that's the easiest one without additional setup
    conn.run_migrations(&migrations).unwrap();
}
