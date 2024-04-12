use super::{users, User};
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
#[cfg(not(feature = "sqlite"))]
use diesel_async::SaveChangesDsl;

#[tokio::test]
#[cfg(feature = "bb8")]
async fn save_changes_bb8() {
    use diesel_async::pooled_connection::bb8::Pool;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;

    let db_url = std::env::var("DATABASE_URL").unwrap();

    let config = AsyncDieselConnectionManager::<super::TestConnection>::new(db_url);
    let pool = Pool::builder().max_size(1).build(config).await.unwrap();

    let mut conn = pool.get().await.unwrap();

    super::setup(&mut *conn).await;

    diesel::insert_into(users::table)
        .values(users::name.eq("John"))
        .execute(&mut conn)
        .await
        .unwrap();

    let u = users::table.first::<User>(&mut conn).await.unwrap();
    assert_eq!(u.name, "John");

    #[cfg(not(feature = "sqlite"))]
    {
        let mut u = u;
        u.name = "Jane".into();
        let u2: User = u.save_changes(&mut conn).await.unwrap();

        assert_eq!(u2.name, "Jane");
    }
}

#[tokio::test]
#[cfg(feature = "deadpool")]
async fn save_changes_deadpool() {
    use diesel_async::pooled_connection::deadpool::Pool;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;

    let db_url = std::env::var("DATABASE_URL").unwrap();

    let config = AsyncDieselConnectionManager::<super::TestConnection>::new(db_url);
    let pool = Pool::builder(config).max_size(1).build().unwrap();

    let mut conn = pool.get().await.unwrap();

    super::setup(&mut *conn).await;

    diesel::insert_into(users::table)
        .values(users::name.eq("John"))
        .execute(&mut conn)
        .await
        .unwrap();

    let u = users::table.first::<User>(&mut conn).await.unwrap();
    assert_eq!(u.name, "John");

    #[cfg(not(feature = "sqlite"))]
    {
        let mut u = u;
        u.name = "Jane".into();
        let u2: User = u.save_changes(&mut conn).await.unwrap();

        assert_eq!(u2.name, "Jane");
    }
}

#[tokio::test]
#[cfg(feature = "mobc")]
async fn save_changes_mobc() {
    use diesel_async::pooled_connection::mobc::Pool;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;

    let db_url = std::env::var("DATABASE_URL").unwrap();

    let config = AsyncDieselConnectionManager::<super::TestConnection>::new(db_url);
    let pool = Pool::new(config);

    let mut conn = pool.get().await.unwrap();

    super::setup(&mut *conn).await;

    diesel::insert_into(users::table)
        .values(users::name.eq("John"))
        .execute(&mut conn)
        .await
        .unwrap();

    let u = users::table.first::<User>(&mut conn).await.unwrap();
    assert_eq!(u.name, "John");

    #[cfg(not(feature = "sqlite"))]
    {
        let mut u = u;
        u.name = "Jane".into();
        let u2: User = u.save_changes(&mut conn).await.unwrap();

        assert_eq!(u2.name, "Jane");
    }
}
