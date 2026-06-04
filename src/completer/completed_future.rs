use std::cell::RefCell;
use std::future::Future;
use std::marker::Unpin;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

/// A future that is immediately ready with a precomputed value.
pub struct CompletedFuture<T: Unpin> {
    data: RefCell<Option<T>>,
}

impl<T: Unpin> CompletedFuture<T> {
    #[allow(dead_code)]
    /// Creates a future that resolves to `data` on the first poll.
    pub fn new(data: T) -> Self {
        Self {
            data: RefCell::new(Some(data)),
        }
    }
}

impl<T: Unpin> Future for CompletedFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _: &mut Context) -> Poll<Self::Output> {
        let data = self.data.borrow_mut().take().unwrap();

        Poll::Ready(data)
    }
}
