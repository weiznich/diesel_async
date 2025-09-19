use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use diesel::QueryResult;
use futures_core::{ready, TryFuture, TryStream};
use futures_util::TryStreamExt;
use pin_project_lite::pin_project;

pin_project! {
    /// Reimplementation of [`futures_util::future::Map`] without the generic closure argument
    #[project = MapProj]
    #[project_replace = MapProjReplace]
    pub enum Map<Fut: Future, T> {
         Incomplete {
            #[pin]
            future: Fut,
            f: fn(Fut::Output) -> QueryResult<T>,
        },
        Complete,
    }
}

impl<Fut: Future, T> Map<Fut, T> {
    pub(crate) fn new(future: Fut, f: fn(Fut::Output) -> QueryResult<T>) -> Self {
        Self::Incomplete { future, f }
    }
}

impl<Fut: Future, T> Future for Map<Fut, T> {
    type Output = QueryResult<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<QueryResult<T>> {
        match self.as_mut().project() {
            MapProj::Incomplete { future, .. } => {
                let output = ready!(future.poll(cx));
                match self.as_mut().project_replace(Map::Complete) {
                    MapProjReplace::Incomplete { f, .. } => Poll::Ready(f(output)),
                    MapProjReplace::Complete => unreachable!(),
                }
            }
            MapProj::Complete => panic!("Map polled after completion"),
        }
    }
}

pin_project! {
    /// Reimplementation of [`futures_util::future::AndThen`] without the generic closure argument
    #[project = AndThenProj]
    pub enum AndThen<Fut1: Future, Fut2> {
        First {
            #[pin]
            future1: Map<Fut1, Fut2>,
        },
        Second {
            #[pin]
            future2: Fut2,
        },
        Empty,
    }
}

impl<Fut1: Future, Fut2> AndThen<Fut1, Fut2> {
    pub(crate) fn new(fut1: Fut1, f: fn(Fut1::Output) -> QueryResult<Fut2>) -> AndThen<Fut1, Fut2> {
        Self::First {
            future1: Map::new(fut1, f),
        }
    }
}

impl<Fut1, Fut2> Future for AndThen<Fut1, Fut2>
where
    Fut1: TryFuture<Error = diesel::result::Error>,
    Fut2: TryFuture<Error = diesel::result::Error>,
{
    type Output = QueryResult<Fut2::Ok>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match self.as_mut().project() {
                AndThenProj::First { future1 } => match ready!(future1.try_poll(cx)) {
                    Ok(future2) => self.set(Self::Second { future2 }),
                    Err(error) => {
                        self.set(Self::Empty);
                        break Poll::Ready(Err(error));
                    }
                },
                AndThenProj::Second { future2 } => {
                    let output = ready!(future2.try_poll(cx));
                    self.set(Self::Empty);
                    break Poll::Ready(output);
                }
                AndThenProj::Empty => panic!("AndThen polled after completion"),
            }
        }
    }
}

/// Converts a stream into a future, only yielding the first element.
/// Based on [`futures_util::stream::StreamFuture`].
pub struct LoadNext<St> {
    stream: Option<St>,
}

impl<St> LoadNext<St> {
    pub(crate) fn new(stream: St) -> Self {
        Self {
            stream: Some(stream),
        }
    }
}

impl<St> Future for LoadNext<St>
where
    St: TryStream<Error = diesel::result::Error> + Unpin,
{
    type Output = QueryResult<St::Ok>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let first = {
            let s = self.stream.as_mut().expect("polling LoadNext twice");
            ready!(s.try_poll_next_unpin(cx))
        };
        self.stream = None;
        match first {
            Some(first) => Poll::Ready(first),
            None => Poll::Ready(Err(diesel::result::Error::NotFound)),
        }
    }
}
