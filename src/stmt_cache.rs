use diesel::connection::statement_cache::{MaybeCached, StatementCallbackReturnType};
use diesel::QueryResult;
use futures_util::{future, FutureExt, TryFutureExt};
use std::future::Future;

pub(crate) struct CallbackHelper<F>(pub(crate) F);

type PrepareFuture<'a, C, S> = future::Either<
    future::Ready<QueryResult<(MaybeCached<'a, S>, C)>>,
    future::BoxFuture<'a, QueryResult<(MaybeCached<'a, S>, C)>>,
>;

impl<'b, S, F, C> StatementCallbackReturnType<S, C> for CallbackHelper<F>
where
    F: Future<Output = QueryResult<(S, C)>> + Send + 'b,
    S: 'static,
{
    type Return<'a> = PrepareFuture<'a, C, S>;

    fn from_error<'a>(e: diesel::result::Error) -> Self::Return<'a> {
        future::Either::Left(future::ready(Err(e)))
    }

    fn map_to_no_cache<'a>(self) -> Self::Return<'a>
    where
        Self: 'a,
    {
        future::Either::Right(
            self.0
                .map_ok(|(stmt, conn)| (MaybeCached::CannotCache(stmt), conn))
                .boxed(),
        )
    }

    fn map_to_cache<'a>(stmt: &'a mut S, conn: C) -> Self::Return<'a> {
        future::Either::Left(future::ready(Ok((MaybeCached::Cached(stmt), conn))))
    }

    fn register_cache<'a>(
        self,
        callback: impl FnOnce(S) -> &'a mut S + Send + 'a,
    ) -> Self::Return<'a>
    where
        Self: 'a,
    {
        future::Either::Right(
            self.0
                .map_ok(|(stmt, conn)| (MaybeCached::Cached(callback(stmt)), conn))
                .boxed(),
        )
    }
}

pub(crate) struct QueryFragmentHelper {
    pub(crate) sql: String,
    pub(crate) safe_to_cache: bool,
}
