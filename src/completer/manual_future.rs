use std::future::Future;
use std::marker::Unpin;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use tracing::error;
use tracing::warn;

enum State<T> {
    Incomplete,
    Waiting(Waker),
    Complete(Option<T>),
}

impl<T> State<T> {
    fn new(value: Option<T>) -> Self {
        match value {
            None => Self::Incomplete,
            v @ Some(_) => Self::Complete(v),
        }
    }
}

/// A future completed manually through the companion completer returned by [`Self::new`].
pub struct ManualFuture<T: Unpin> {
    state: Arc<Mutex<State<T>>>,
}

impl<T: Unpin> ManualFuture<T> {
    /// Creates a pending future together with the completer that resolves it.
    pub fn new() -> (Self, ManualFutureCompleter<T>) {
        let state: State<T> = State::new(None);
        let a = Arc::new(Mutex::new(state));
        (
            Self { state: a.clone() },
            ManualFutureCompleter { state: a },
        )
    }

    #[allow(dead_code)]
    /// Returns `true` once the future has been completed or cancelled.
    pub fn is_completed(&self) -> bool {
        let state = self.state.lock().unwrap();

        match *state {
            State::Incomplete => false,
            State::Waiting(_) => false,
            State::Complete(_) => true,
        }
    }
}

impl<T: Unpin> Clone for ManualFuture<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

/// Completes or cancels a [`ManualFuture`].
pub struct ManualFutureCompleter<T: Unpin> {
    state: Arc<Mutex<State<T>>>,
}

impl<T: Unpin> ManualFutureCompleter<T> {
    /// Resolves the paired future with `value`.
    pub fn complete(self, value: T) {
        let mut state = self.state.lock().unwrap();

        match std::mem::replace(&mut *state, State::Complete(Some(value))) {
            State::Incomplete => {}
            State::Waiting(w) => w.wake(),
            _ => panic!("Future is completed or cancelled already"),
        }
    }

    /// Cancels the paired future without producing a value.
    pub fn cancel(self) {
        warn!("Cancelling future...");
        let mut state = self.state.lock().unwrap();
        *state = State::Complete(None);
    }

    #[allow(dead_code)]
    /// Returns `true` once the paired future has been completed or cancelled.
    pub fn is_completed(&self) -> bool {
        let state = self.state.lock().unwrap();

        match *state {
            State::Incomplete => false,
            State::Waiting(_) => false,
            State::Complete(_) => true,
        }
    }
}

impl<T: Unpin> Clone for ManualFutureCompleter<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T: Unpin> Future for ManualFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut state = self.state.lock().unwrap();

        match &mut *state {
            s @ State::Incomplete => *s = State::Waiting(cx.waker().clone()),
            State::Waiting(w) if w.will_wake(cx.waker()) => {}
            s @ State::Waiting(_) => *s = State::Waiting(cx.waker().clone()),
            State::Complete(v) => match v.take() {
                Some(v) => return Poll::Ready(v),
                None => {
                    error!("Future is cancelled or double polled...");
                }
            },
        }

        Poll::Pending
    }
}
