use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use diesel::QueryResult;
use futures_core::{TryFuture, TryStream};
use futures_util::stream::TryCollect;
use futures_util::{TryFutureExt, TryStreamExt};

// We use a custom future implementation here to erase some lifetimes
// that otherwise need to be specified explicitly
//
// Specifying these lifetimes results in the compiler not beeing
// able to look through the generic code and emit
// lifetime erros for pipelined queries. See
// https://github.com/weiznich/diesel_async/issues/249 for more context
#[repr(transparent)]
pub struct MapOk<F: TryFutureExt, T> {
    future: futures_util::future::MapOk<F, fn(F::Ok) -> T>,
}

impl<F, T> Future for MapOk<F, T>
where
    F: TryFuture,
    futures_util::future::MapOk<F, fn(F::Ok) -> T>: Future<Output = Result<T, F::Error>>,
{
    type Output = Result<T, F::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        unsafe {
            // SAFETY: This projects pinning to the only inner field, so it
            // should be safe
            self.map_unchecked_mut(|s| &mut s.future)
        }
        .poll(cx)
    }
}

impl<Fut: TryFutureExt, T> MapOk<Fut, T> {
    pub(crate) fn new(future: Fut, f: fn(Fut::Ok) -> T) -> Self {
        Self {
            future: future.map_ok(f),
        }
    }
}

// similar to `MapOk` above this mainly exists to hide the lifetime
#[repr(transparent)]
pub struct AndThen<F1: TryFuture, F2> {
    future: futures_util::future::AndThen<F1, F2, fn(F1::Ok) -> F2>,
}

impl<Fut1, Fut2> AndThen<Fut1, Fut2>
where
    Fut1: TryFuture,
    Fut2: TryFuture<Error = Fut1::Error>,
{
    pub(crate) fn new(fut1: Fut1, f: fn(Fut1::Ok) -> Fut2) -> AndThen<Fut1, Fut2> {
        Self {
            future: fut1.and_then(f),
        }
    }
}

impl<F1, F2> Future for AndThen<F1, F2>
where
    F1: TryFuture,
    F2: TryFuture<Error = F1::Error>,
{
    type Output = Result<F2::Ok, F2::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe {
            // SAFETY: This projects pinning to the only inner field, so it
            // should be safe
            self.map_unchecked_mut(|s| &mut s.future)
        }
        .poll(cx)
    }
}

/// Converts a stream into a future, only yielding the first element.
/// Based on [`futures_util::stream::StreamFuture`].
///
/// Consumes the entire stream to ensure proper cleanup before returning which is
/// required to fix: https://github.com/weiznich/diesel_async/issues/269
#[repr(transparent)]
pub struct LoadNext<F, T>
where
    F: TryStream<Ok = T, Error = diesel::result::Error>,
{
    future: TryCollect<F, Vec<T>>,
}

impl<F, T> LoadNext<F, T>
where
    F: TryStream<Ok = T, Error = diesel::result::Error>,
{
    pub(crate) fn new(stream: F) -> Self {
        Self {
            future: stream.try_collect(),
        }
    }
}

impl<F, T> Future for LoadNext<F, T>
where
    F: TryStream<Ok = T, Error = diesel::result::Error>,
    TryCollect<F, Vec<T>>: Future<Output = Result<Vec<T>, diesel::result::Error>>,
{
    type Output = QueryResult<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match unsafe {
            // SAFETY: This projects pinning to the only inner field
            self.map_unchecked_mut(|s| &mut s.future)
        }
        .poll(cx)
        {
            Poll::Ready(Ok(results)) => match results.into_iter().next() {
                Some(first) => Poll::Ready(Ok(first)),
                None => Poll::Ready(Err(diesel::result::Error::NotFound)),
            },
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}
