use diesel::connection::{MaybeCached, PrepareForCache};
use diesel::pg::{
    FailedToLookupTypeError, PgMetadataCache, PgMetadataCacheKey, PgMetadataLookup, PgTypeMetadata,
};
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::query_builder::{AsQuery, QueryFragment, QueryId};
use diesel::{ConnectionError, ConnectionResult, QueryResult};
use futures::stream::TryStreamExt;
use futures::{Future, Stream, StreamExt};
use std::borrow::Cow;
use std::pin::Pin;
use tokio_postgres::types::ToSql;
use tokio_postgres::types::Type;
use tokio_postgres::Statement;

mod error_helper;
mod row;
mod serialize;
mod transaction_builder;

use self::error_helper::ErrorHelper;
use self::row::PgRow;
use self::serialize::ToSqlHelper;

pub use self::transaction_builder::TransactionBuilder;

use crate::stmt_cache::{PrepareCallback, StmtCache};
use crate::{
    AnsiTransactionManager, AsyncConnection, AsyncConnectionGatWorkaround, SimpleAsyncConnection,
    TransactionManager,
};

pub struct AsyncPgConnection {
    conn: tokio_postgres::Client,
    stmt_cache: StmtCache<diesel::pg::Pg, Statement>,
    transaction_state: AnsiTransactionManager,
    metadata_cache: PgMetadataCache,
    next_lookup: Vec<(Option<String>, String)>,
}

#[async_trait::async_trait]
impl SimpleAsyncConnection for AsyncPgConnection {
    async fn batch_execute(&mut self, query: &str) -> QueryResult<()> {
        Ok(self.conn.batch_execute(query).await.map_err(ErrorHelper)?)
    }
}

impl<'a> AsyncConnectionGatWorkaround<'a, diesel::pg::Pg> for AsyncPgConnection {
    type Stream = Pin<Box<dyn Stream<Item = QueryResult<PgRow>> + Send + 'a>>;

    type Row = PgRow;
}

#[async_trait::async_trait]
impl AsyncConnection for AsyncPgConnection {
    type Backend = diesel::pg::Pg;
    type TransactionManager = AnsiTransactionManager;

    async fn establish(database_url: &str) -> ConnectionResult<Self> {
        let (client, connection) = tokio_postgres::connect(database_url, tokio_postgres::NoTls)
            .await
            .map_err(ErrorHelper)?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        let mut conn = AsyncPgConnection {
            conn: client,
            stmt_cache: StmtCache::new(),
            transaction_state: AnsiTransactionManager::default(),
            metadata_cache: PgMetadataCache::new(),
            next_lookup: Vec::new(),
        };
        conn.set_config_options()
            .await
            .map_err(ConnectionError::CouldntSetupConfiguration)?;
        Ok(conn)
    }

    async fn execute(&mut self, query: &str) -> QueryResult<usize> {
        let res = tokio_postgres::Client::execute(&self.conn, query, &[])
            .await
            .map_err(ErrorHelper)?;
        Ok(res as usize)
    }

    async fn load<'a, T>(
        &'a mut self,
        source: T,
    ) -> QueryResult<<Self as AsyncConnectionGatWorkaround<'a, Self::Backend>>::Stream>
    where
        T: AsQuery + Send,
        T::Query: QueryFragment<Self::Backend> + QueryId + Send,
    {
        self.with_preapared_statement(source.as_query(), |conn, stmt, binds| async move {
            let res = conn.query_raw(&*stmt, binds).await.map_err(ErrorHelper)?;

            Ok(res
                .map_err(|e| diesel::result::Error::from(ErrorHelper(e)))
                .map_ok(PgRow::new)
                .boxed())
        })
        .await
    }

    async fn execute_returning_count<T>(&mut self, source: T) -> QueryResult<usize>
    where
        T: QueryFragment<Self::Backend> + QueryId + Send,
    {
        self.with_preapared_statement(source, |conn, stmt, binds| async move {
            let binds = binds
                .iter()
                .map(|b| b as &(dyn ToSql + Sync))
                .collect::<Vec<_>>();

            let res = tokio_postgres::Client::execute(conn, &*stmt, &binds as &[_])
                .await
                .map_err(ErrorHelper)?;
            Ok(res as usize)
        })
        .await
    }

    fn transaction_state(
        &mut self,
    ) -> &mut <Self::TransactionManager as TransactionManager<Self>>::TransactionStateData {
        &mut self.transaction_state
    }

    async fn transaction<F, R, E>(&mut self, callback: F) -> Result<R, E>
    where
        F: FnOnce(&mut Self) -> futures::future::BoxFuture<Result<R, E>> + Send,
        E: From<diesel::result::Error> + Send,
        R: Send,
    {
        Self::TransactionManager::begin_transaction(self).await?;
        match callback(&mut *self).await {
            Ok(value) => {
                Self::TransactionManager::commit_transaction(self).await?;
                Ok(value)
            }
            Err(e) => {
                Self::TransactionManager::rollback_transaction(self)
                    .await
                    .map_err(|e| diesel::result::Error::RollbackError(Box::new(e)))?;
                Err(e)
            }
        }
    }

    async fn begin_test_transaction(&mut self) -> QueryResult<()> {
        assert_eq!(Self::TransactionManager::get_transaction_depth(self), 0);
        Self::TransactionManager::begin_transaction(self).await
    }
}

#[async_trait::async_trait]
impl PrepareCallback<Statement, PgTypeMetadata> for tokio_postgres::Client {
    async fn prepare(
        &mut self,
        sql: &str,
        metadata: &[PgTypeMetadata],
        _is_for_cache: PrepareForCache,
    ) -> QueryResult<Statement> {
        let bind_types = metadata
            .iter()
            .map(|t| type_from_oid(t))
            .collect::<QueryResult<Vec<_>>>()?;

        Ok(self
            .prepare_typed(sql, &bind_types)
            .await
            .map_err(ErrorHelper)?)
    }
}

fn type_from_oid(t: &PgTypeMetadata) -> QueryResult<Type> {
    let oid = t
        .oid()
        .map_err(|e| diesel::result::Error::SerializationError(Box::new(e) as _))?;

    if let Some(tpe) = Type::from_oid(oid) {
        return Ok(tpe);
    }

    Ok(Type::new(
        "diesel_custom_type".into(),
        oid,
        tokio_postgres::types::Kind::Simple,
        "public".into(),
    ))
}

impl AsyncPgConnection {
    pub fn build_transaction(&mut self) -> TransactionBuilder<Self> {
        TransactionBuilder::new(self)
    }

    pub async fn try_from(conn: tokio_postgres::Client) -> ConnectionResult<Self> {
        let mut conn = Self {
            conn,
            stmt_cache: StmtCache::new(),
            transaction_state: AnsiTransactionManager::default(),
            metadata_cache: PgMetadataCache::new(),
            next_lookup: Vec::new(),
        };
        conn.set_config_options()
            .await
            .map_err(ConnectionError::CouldntSetupConfiguration)?;
        Ok(conn)
    }

    async fn set_config_options(&mut self) -> QueryResult<()> {
        self.execute("SET TIME ZONE 'UTC'").await?;
        self.execute("SET CLIENT_ENCODING TO 'UTF8'").await?;
        Ok(())
    }

    async fn with_preapared_statement<'a, T, F, R>(
        &'a mut self,
        query: T,
        callback: impl FnOnce(
            &'a mut tokio_postgres::Client,
            MaybeCached<'a, Statement>,
            Vec<ToSqlHelper>,
        ) -> F,
    ) -> QueryResult<R>
    where
        T: QueryFragment<diesel::pg::Pg> + QueryId + Send,
        F: Future<Output = QueryResult<R>>,
    {
        let mut bind_collector;
        loop {
            // we need a new bind collector per iteration here
            bind_collector = RawBytesBindCollector::<diesel::pg::Pg>::new();
            let res = query.collect_binds(&mut bind_collector, self);

            if !self.next_lookup.is_empty() {
                for (schema, lookup_type_name) in
                    std::mem::replace(&mut self.next_lookup, Vec::new())
                {
                    // as this is an async call and we don't want to infect the whole diesel serialization
                    // api with async we just error out in the `PgMetadataLookup` implementation below if we encounter
                    // a type that is not cached yet
                    // If that's the case we will do the lookup here and try again as the
                    // type is now cached.
                    let type_metadata =
                        lookup_type(schema.clone(), lookup_type_name.clone(), self).await?;
                    self.metadata_cache.store_type(
                        PgMetadataCacheKey::new(
                            schema.map(Cow::Owned),
                            Cow::Owned(lookup_type_name),
                        ),
                        type_metadata,
                    );
                    // just try again to get the binds, now that we've inserted the
                    // type into the lookup list
                }
            } else {
                // bubble up any error as soon as we have done all lookups
                let _ = res?;
                break;
            }
        }

        let AsyncPgConnection {
            ref mut conn,
            ref mut stmt_cache,
            ..
        } = self;

        let conn = &mut *conn;

        let stmt = {
            let stmt = stmt_cache
                .cached_prepared_statement(query, &bind_collector.metadata, conn)
                .await?;
            stmt
        };

        let binds = bind_collector
            .metadata
            .into_iter()
            .zip(bind_collector.binds)
            .map(|(meta, bind)| ToSqlHelper(meta, bind))
            .collect::<Vec<_>>();

        callback(&mut self.conn, stmt, binds).await
    }
}

impl PgMetadataLookup for AsyncPgConnection {
    fn lookup_type(&mut self, type_name: &str, schema: Option<&str>) -> PgTypeMetadata {
        let cache_key =
            PgMetadataCacheKey::new(schema.map(Cow::Borrowed), Cow::Borrowed(type_name));

        if let Some(metadata) = self.metadata_cache.lookup_type(&cache_key) {
            metadata
        } else {
            let cache_key = cache_key.into_owned();
            self.next_lookup
                .push((schema.map(ToOwned::to_owned), type_name.to_owned()));
            PgTypeMetadata::from_result(Err(FailedToLookupTypeError::new(cache_key)))
        }
    }
}

async fn lookup_type(
    schema: Option<String>,
    type_name: String,
    conn: &mut AsyncPgConnection,
) -> QueryResult<(u32, u32)> {
    use crate::RunQueryDsl;
    use diesel::prelude::{BoolExpressionMethods, ExpressionMethods, QueryDsl};

    let search_path: String;
    let mut search_path_has_temp_schema = false;

    let search_schema = match schema.as_ref().map(|a| a as &str) {
        Some("pg_temp") => {
            search_path_has_temp_schema = true;
            Vec::new()
        }
        Some(schema) => vec![schema],
        None => {
            search_path = diesel::dsl::sql::<diesel::sql_types::Text>("SHOW search_path")
                .get_result::<String>(conn)
                .await?;

            search_path
                .split(',')
                // skip the `$user` entry for now
                .filter(|f| !f.starts_with("\"$"))
                .map(|s| s.trim())
                .filter(|&f| {
                    let is_temp = f == "pg_temp";
                    search_path_has_temp_schema |= is_temp;
                    !is_temp
                })
                .collect()
        }
    };

    let metadata_query = pg_type::table
        .inner_join(pg_namespace::table)
        .filter(pg_type::typname.eq(type_name))
        .select((pg_type::oid, pg_type::typarray));
    let nspname_filter = pg_namespace::nspname.eq_any(search_schema);

    let metadata = if search_path_has_temp_schema {
        metadata_query
            .filter(nspname_filter.or(pg_namespace::oid.eq(pg_my_temp_schema())))
            .first(conn)
            .await?
    } else {
        metadata_query.filter(nspname_filter).first(conn).await?
    };

    QueryResult::Ok(metadata)
}

diesel::table! {
    pg_type (oid) {
        oid -> Oid,
        typname -> Text,
        typarray -> Oid,
        typnamespace -> Oid,
    }
}

diesel::table! {
    pg_namespace (oid) {
        oid -> Oid,
        nspname -> Text,
    }
}

diesel::joinable!(pg_type -> pg_namespace(typnamespace));
diesel::allow_tables_to_appear_in_same_query!(pg_type, pg_namespace);

diesel::sql_function! { fn pg_my_temp_schema() -> Oid; }
