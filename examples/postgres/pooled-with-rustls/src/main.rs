use diesel::{ConnectionError, ConnectionResult};
use diesel_async::pooled_connection::bb8::Pool;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::ManagerConfig;
use diesel_async::AsyncPgConnection;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_url = std::env::var("DATABASE_URL").expect("Env var `DATABASE_URL` not set");

    let mut config = ManagerConfig::default();
    config.custom_setup = Box::new(establish_connection);

    // First we have to construct a connection manager with our custom `establish_connection`
    // function
    let mgr = AsyncDieselConnectionManager::<AsyncPgConnection>::new_with_config(db_url, config);
    // From that connection we can then create a pool, here given with some example settings.
    //
    // This creates a TLS configuration that's equivalent to `libpq'` `sslmode=verify-full`, which
    // means this will check whether the provided certificate is valid for the given database host.
    //
    // `libpq` does not perform these checks by default (https://www.postgresql.org/docs/current/libpq-connect.html)
    // If you hit a TLS error while conneting to the database double check your certificates
    let pool = Pool::builder()
        .max_size(10)
        .min_idle(Some(5))
        .max_lifetime(Some(Duration::from_secs(60 * 60 * 24)))
        .idle_timeout(Some(Duration::from_secs(60 * 2)))
        .build(mgr)
        .await?;

    // Now we can use our pool to run queries over a TLS-secured connection:
    let conn = pool.get().await?;
    let _ = conn;

    Ok(())
}

fn establish_connection(config: &str) -> BoxFuture<ConnectionResult<AsyncPgConnection>> {
    let fut = async {
        // We first set up the way we want rustls to work.
        let rustls_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_certs())
            .with_no_client_auth();
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(rustls_config);
        let (client, conn) = tokio_postgres::connect(config, tls)
            .await
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        AsyncPgConnection::try_from_client_and_connection(client, conn).await
    };
    fut.boxed()
}

fn root_certs() -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    let certs = rustls_native_certs::load_native_certs().expect("Certs not loadable!");
    roots.add_parsable_certificates(certs);
    roots
}
