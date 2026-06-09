use futures::Stream;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use tracing::error;

struct ManualStreamState<T> {
    queue: Arc<Mutex<VecDeque<Option<T>>>>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl<T> Clone for ManualStreamState<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            waker: self.waker.clone(),
        }
    }
}

impl<T> ManualStreamState<T> {
    fn new() -> Self {
        ManualStreamState {
            queue: Arc::new(Mutex::new(VecDeque::new())),
            waker: Arc::new(Mutex::new(None)),
        }
    }

    fn queue_guard(&self) -> MutexGuard<'_, VecDeque<Option<T>>> {
        match self.queue.lock() {
            Ok(queue) => queue,
            Err(poisoned) => {
                error!("ManualStream queue mutex poisoned");
                poisoned.into_inner()
            }
        }
    }

    fn waker_guard(&self) -> MutexGuard<'_, Option<Waker>> {
        match self.waker.lock() {
            Ok(waker) => waker,
            Err(poisoned) => {
                error!("ManualStream waker mutex poisoned");
                poisoned.into_inner()
            }
        }
    }

    fn push(&self, item: T) {
        let mut queue = self.queue_guard();
        queue.push_back(Some(item));
        if let Some(waker) = self.waker_guard().take() {
            waker.wake();
        }
    }

    fn close(&self) {
        let mut queue = self.queue_guard();
        queue.push_back(None);
        if let Some(waker) = self.waker_guard().take() {
            waker.wake();
        }
    }
}

/// A stream fed manually through the companion completer returned by [`Self::create`].
pub struct ManualStream<T> {
    state: ManualStreamState<T>,
}

impl<T> ManualStream<T> {
    /// Creates an empty stream and the completer that pushes items into it.
    #[must_use]
    pub fn create() -> (Self, ManualStreamCompleter<T>) {
        let state = ManualStreamState::new();

        (
            ManualStream {
                state: state.clone(),
            },
            ManualStreamCompleter { state },
        )
    }
}

/// Pushes items into a [`ManualStream`] and closes it when no more items remain.
pub struct ManualStreamCompleter<T> {
    state: ManualStreamState<T>,
}

impl<T> ManualStreamCompleter<T> {
    /// Queues one item for the paired stream.
    ///
    pub fn push(&self, item: T) {
        self.state.push(item);
    }

    /// Finishes the paired stream.
    ///
    pub fn close(&self) {
        self.state.close();
    }
}

impl<T> Stream for ManualStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut queue = self.state.queue_guard();
        if let Some(item) = queue.pop_front() {
            match item {
                Some(value) => Poll::Ready(Some(value)),
                None => Poll::Ready(None),
            }
        } else {
            let mut waker = self.state.waker_guard();
            *waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
