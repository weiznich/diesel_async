use diesel_async::*;
use diesel::prelude::*;

cfg_if::cfg_if! {
    if #[cfg(feature = "postgres")] {
        #[allow(dead_code)]
        type DB = diesel::pg::Pg;

        async fn connection_no_transaction() -> AsyncPgConnection {
            let connection_url = database_url_from_env("PG_DATABASE_URL");
            AsyncPgConnection::establish(&connection_url).await.unwrap()
        }

        async fn clear_tables(connection: &mut AsyncPgConnection) {
            diesel::sql_query("DROP TABLE IF EXISTS users CASCADE").execute(connection).await.unwrap();
            diesel::sql_query("DROP TABLE IF EXISTS animals CASCADE").execute(connection).await.unwrap();
            diesel::sql_query("DROP TABLE IF EXISTS posts CASCADE").execute(connection).await.unwrap();
            diesel::sql_query("DROP TABLE IF EXISTS comments CASCADE").execute(connection).await.unwrap();
            diesel::sql_query("DROP TABLE IF EXISTS brands CASCADE").execute(connection).await.unwrap();
        }

        async fn connection_no_data() -> AsyncPgConnection {
            let mut connection = connection_no_transaction().await;
            connection.begin_test_transaction().await.unwrap();
            clear_tables(&mut connection).await;
            connection
        }

        async fn create_tables(connection: &mut AsyncPgConnection) {
            diesel::sql_query("CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR NOT NULL
            )").execute(connection).await.unwrap();
            diesel::sql_query(
                "INSERT INTO users (name) VALUES ('Sean'), ('Tess')"
            ).execute(connection).await.unwrap();

            diesel::sql_query(
                "CREATE TABLE IF NOT EXISTS animals (
                id SERIAL PRIMARY KEY,
                species VARCHAR NOT NULL,
                legs INTEGER NOT NULL,
                name VARCHAR
            )").execute(connection).await.unwrap();
            diesel::sql_query(
                "INSERT INTO animals (species, legs, name) VALUES
                               ('dog', 4, 'Jack'),
                               ('spider', 8, null)"
            ).execute(connection)
             .await.unwrap();

            diesel::sql_query(
                "CREATE TABLE IF NOT EXISTS posts (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL,
                title VARCHAR NOT NULL
            )").execute(connection).await.unwrap();
            diesel::sql_query(
                "INSERT INTO posts (user_id, title) VALUES
                (1, 'My first post'),
                (1, 'About Rust'),
                (2, 'My first post too')").execute(connection).await.unwrap();

            diesel::sql_query("CREATE TABLE IF NOT EXISTS comments (
                id SERIAL PRIMARY KEY,
                post_id INTEGER NOT NULL,
                body VARCHAR NOT NULL
            )").execute(connection).await.unwrap();
            diesel::sql_query("INSERT INTO comments (post_id, body) VALUES
                (1, 'Great post'),
                (2, 'Yay! I am learning Rust'),
                (3, 'I enjoyed your post')").execute(connection).await.unwrap();

            diesel::sql_query("CREATE TABLE IF NOT EXISTS brands (
                id SERIAL PRIMARY KEY,
                color VARCHAR NOT NULL DEFAULT 'Green',
                accent VARCHAR DEFAULT 'Blue'
            )").execute(connection).await.unwrap();
        }

        #[allow(dead_code)]
        async fn establish_connection() -> AsyncPgConnection {
            let mut connection = connection_no_data().await;
            create_tables(&mut connection).await;
            connection
        }
    }  else if #[cfg(feature = "mysql")] {
        #[allow(dead_code)]
        type DB = diesel::mysql::Mysql;

        async fn clear_tables(connection: &mut AsyncMysqlConnection) {
            diesel::sql_query("SET FOREIGN_KEY_CHECKS=0;").execute(connection).await.unwrap();
            diesel::sql_query("DROP TABLE IF EXISTS users CASCADE").execute(connection).await.unwrap();
            diesel::sql_query("DROP TABLE IF EXISTS animals CASCADE").execute(connection).await.unwrap();
            diesel::sql_query("DROP TABLE IF EXISTS posts CASCADE").execute(connection).await.unwrap();
            diesel::sql_query("DROP TABLE IF EXISTS comments CASCADE").execute(connection).await.unwrap();
            diesel::sql_query("DROP TABLE IF EXISTS brands CASCADE").execute(connection).await.unwrap();
            diesel::sql_query("SET FOREIGN_KEY_CHECKS=1;").execute(connection).await.unwrap();
        }

        async fn connection_no_data() -> AsyncMysqlConnection {
            let connection_url = database_url_from_env("MYSQL_UNIT_TEST_DATABASE_URL");
            let mut connection = AsyncMysqlConnection::establish(&connection_url).await.unwrap();
            clear_tables(&mut connection).await;
            connection
        }

        async fn create_tables(connection: &mut AsyncMysqlConnection) {
               diesel::sql_query("CREATE TEMPORARY TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTO_INCREMENT,
                name TEXT NOT NULL
            ) CHARACTER SET utf8mb4").execute(connection).await.unwrap();


            diesel::sql_query("CREATE TEMPORARY TABLE IF NOT EXISTS animals (
                id INTEGER PRIMARY KEY AUTO_INCREMENT,
                species TEXT NOT NULL,
                legs INTEGER NOT NULL,
                name TEXT
            ) CHARACTER SET utf8mb4").execute(connection).await.unwrap();

            diesel::sql_query("CREATE TEMPORARY TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTO_INCREMENT,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL
            ) CHARACTER SET utf8mb4").execute(connection).await.unwrap();

            diesel::sql_query("CREATE TEMPORARY TABLE IF NOT EXISTS comments (
                id INTEGER PRIMARY KEY AUTO_INCREMENT,
                post_id INTEGER NOT NULL,
                body TEXT NOT NULL
            ) CHARACTER SET utf8mb4").execute(connection).await.unwrap();
            diesel::sql_query("CREATE TEMPORARY TABLE IF NOT EXISTS brands (
                id INTEGER PRIMARY KEY AUTO_INCREMENT,
                color VARCHAR(255) NOT NULL DEFAULT 'Green',
                accent VARCHAR(255) DEFAULT 'Blue'
            )").execute(connection).await.unwrap();

            connection.begin_test_transaction().await.unwrap();
            diesel::sql_query("INSERT INTO users (name) VALUES ('Sean'), ('Tess')").execute(connection).await.unwrap();
            diesel::sql_query("INSERT INTO posts (user_id, title) VALUES
                (1, 'My first post'),
                (1, 'About Rust'),
                (2, 'My first post too')").execute(connection).await.unwrap();
            diesel::sql_query("INSERT INTO comments (post_id, body) VALUES
                (1, 'Great post'),
                (2, 'Yay! I am learning Rust'),
                (3, 'I enjoyed your post')").execute(connection).await.unwrap();
            diesel::sql_query("INSERT INTO animals (species, legs, name) VALUES
                               ('dog', 4, 'Jack'),
                               ('spider', 8, null)").execute(connection).await.unwrap();

        }

        #[allow(dead_code)]
        async fn establish_connection() -> AsyncMysqlConnection {
            let mut connection = connection_no_data().await;
            create_tables(&mut connection).await;


            connection
        }
    } else {
        compile_error!(
            "At least one backend must be used to test this crate.\n \
            Pass argument `--features \"<backend>\"` with one or more of the following backends, \
            'mysql', 'postgres', or 'sqlite'. \n\n \
            ex. cargo test --features \"mysql postgres sqlite\"\n"
        );
    }
}

fn database_url_from_env(backend_specific_env_var: &str) -> String {
    use std::env;

    //dotenv().ok();

    env::var(backend_specific_env_var)
        .or_else(|_| env::var("DATABASE_URL"))
        .expect("DATABASE_URL must be set in order to run tests")
}

mod schema {
    use diesel::prelude::*;

    table! {
        animals {
            id -> Integer,
            species -> VarChar,
            legs -> Integer,
            name -> Nullable<VarChar>,
        }
    }

    table! {
        comments {
            id -> Integer,
            post_id -> Integer,
            body -> VarChar,
        }
    }

    table! {
        posts {
            id -> Integer,
            user_id -> Integer,
            title -> VarChar,
        }
    }

    table! {
        users {
            id -> Integer,
            name -> VarChar,
        }
    }

    #[cfg(not(feature = "sqlite"))]
    table! {
        brands {
            id -> Integer,
            color -> VarChar,
            accent -> Nullable<VarChar>,
        }
    }

    joinable!(posts -> users (user_id));
    allow_tables_to_appear_in_same_query!(animals, comments, posts, users);
}
