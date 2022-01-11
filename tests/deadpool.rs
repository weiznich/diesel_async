use diesel::prelude::ExpressionMethods;
use diesel_async::*;
use futures::future::join_all;

#[cfg(feature = "deadpool")]
use diesel_async::deadpool::{ConnectionManager, ManagedAsyncConnection, Pool};

use schema::{users, User};

#[cfg(feature = "postgres")]
type TestConnection = AsyncPgConnection;

#[cfg(all(feature = "mysql", not(feature = "postgres")))]
type TestConnection = AsyncMysqlConnection;

#[cfg(feature = "deadpool")]
#[tokio::test]
async fn pool_usage() {
    let pool = _pool::<TestConnection>().await;
    let mut conn = pool.get().await.expect("unable get");

    diesel::insert_into(users::table)
        .values([users::name.eq("John Doe"), users::name.eq("Jane Doe")])
        .execute(&mut conn)
        .await
        .unwrap();

    // 10 requests to users table in parallel
    let loads = (0..10).map(|_| async {
        let mut conn = pool.get().await.expect("unable get");

        users::table
            .load::<User>(&mut conn)
            .await
            .expect("unable load")
    });

    let loads: Vec<_> = join_all(loads).await;
    assert_eq!(loads.len(), 10);

    for users in loads {
        assert_eq!(&users[0].name, "John Doe", "User name [0] does not match");
        assert_eq!(&users[1].name, "Jane Doe", "User name [1] does not match");
    }
}

#[cfg(feature = "deadpool")]
async fn _pool<T: ManagedAsyncConnection>() -> Pool<T> {
    let db_url = std::env::var("DATABASE_URL").expect("unable var");
    let manager = ConnectionManager::<T>::new(db_url);

    Pool::builder(manager)
        .max_size(10)
        .build()
        .expect("unable build")
}

mod schema {
    diesel::table! {
        users {
            id -> Integer,
            name -> Text,
        }
    }

    #[derive(diesel::Queryable, diesel::Selectable, Debug, PartialEq)]
    pub(crate) struct User {
        pub(crate) id: i32,
        pub(crate) name: String,
    }
}
