//! Cooperative cancellation triggered by SIGINT / SIGTERM.
//!
//! [`Shutdown`] is the source of truth, owned by the top-level daemon
//! task. [`ShutdownToken`] is the cheap clonable handle that connection
//! tasks, the accept loop, and any other long-running work hold; they
//! await [`ShutdownToken::cancelled`] alongside their own work via
//! `tokio::select!`.

use std::sync::Arc;

use tokio::sync::watch;

/// Source of the cancellation signal. One per daemon process.
#[derive(Debug)]
pub struct Shutdown {
    tx: watch::Sender<bool>,
    /// Handle to the SIGINT/SIGTERM listener spawned by
    /// [`Self::install_signal_handlers`], if any. Held so it can be
    /// aborted on `trigger()` / `Drop` rather than leaked: on the
    /// programmatic-trigger path (tests, embedders) no OS signal ever
    /// fires, so without this the parked task and its two signal-stream
    /// registrations would live for the runtime's whole lifetime (L10).
    signal_task: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Cheap handle to the cancellation signal. Cloneable, lightweight.
#[derive(Clone, Debug)]
pub struct ShutdownToken {
    rx: watch::Receiver<bool>,
}

impl Shutdown {
    /// Create a new shutdown signal. Initially un-cancelled.
    #[must_use]
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(false);
        Self {
            tx,
            signal_task: std::sync::Mutex::new(None),
        }
    }

    /// Issue a cheap clonable token.
    #[must_use]
    pub fn token(&self) -> ShutdownToken {
        ShutdownToken {
            rx: self.tx.subscribe(),
        }
    }

    /// Trigger cancellation. Idempotent.
    pub fn trigger(&self) {
        // send_replace ignores the case where there are no receivers.
        let _ = self.tx.send_replace(true);
        // Abort the signal listener (L10): its only job is to call
        // `trigger()`, which has now happened by another route, so it
        // has nothing left to do. Aborting frees its task + signal
        // registrations instead of leaving it parked on `recv()` for the
        // runtime's lifetime. Idempotent: a second trigger finds None.
        let task = self.signal_task.lock().expect("poisoned").take();
        if let Some(handle) = task {
            handle.abort();
        }
    }

    /// Whether shutdown has already been triggered.
    #[must_use]
    pub fn is_triggered(&self) -> bool {
        *self.tx.borrow()
    }

    /// Spawn a task that waits for SIGINT or SIGTERM and triggers
    /// shutdown when either fires. The returned [`Arc<Shutdown>`] is the
    /// same handle the caller passes in; it is cloned into the spawned
    /// task so the signal listener outlives nothing in particular.
    ///
    /// Returns an `io::Error` if either signal stream fails to install
    /// (extremely unlikely on Unix; would mean the process has no signal
    /// handling at all).
    pub fn install_signal_handlers(self: &Arc<Self>) -> std::io::Result<()> {
        use tokio::signal::unix::{SignalKind, signal};

        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;
        // Hold a `Weak`, not an `Arc`: an `Arc` here would be a cycle
        // (Shutdown → signal_task JoinHandle → task → Arc<Shutdown>) that
        // keeps the parked task — and `Shutdown` itself — alive forever
        // if the owner drops without calling `trigger()`. With a `Weak`,
        // dropping the last real `Arc<Shutdown>` runs `Drop`, which
        // aborts this task (L10).
        let me = Arc::downgrade(self);
        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = sigint.recv() => {
                    tracing::info!("received SIGINT, shutting down");
                }
                _ = sigterm.recv() => {
                    tracing::info!("received SIGTERM, shutting down");
                }
            }
            if let Some(me) = me.upgrade() {
                me.trigger();
            }
        });
        *self.signal_task.lock().expect("poisoned") = Some(handle);
        Ok(())
    }

    /// Test-only accessor for the spawned signal-listener handle, so the
    /// suite can assert it terminates on `trigger()` (L10).
    #[cfg(test)]
    fn signal_task_handle(&self) -> Option<tokio::task::AbortHandle> {
        self.signal_task
            .lock()
            .expect("poisoned")
            .as_ref()
            .map(tokio::task::JoinHandle::abort_handle)
    }
}

impl Drop for Shutdown {
    fn drop(&mut self) {
        // Abort the signal listener if it's still parked (e.g. owner
        // dropped without an explicit `trigger()`), so it doesn't outlive
        // the daemon it served. The task holds only a `Weak<Shutdown>`,
        // so reaching this `Drop` is possible while the task is alive.
        let task = self.signal_task.lock().expect("poisoned").take();
        if let Some(handle) = task {
            handle.abort();
        }
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownToken {
    /// Resolve when shutdown is triggered. If already triggered,
    /// resolves immediately.
    pub async fn cancelled(&mut self) {
        // If already triggered, return immediately without waiting on
        // the watch channel.
        if *self.rx.borrow() {
            return;
        }
        // Wait for the next change. `changed` returns Err only when the
        // sender is dropped, which we treat as "shut down" too — there's
        // no source of cancellation left.
        let _ = self.rx.changed().await;
    }

    /// Snapshot of the current state. Cheap.
    #[must_use]
    pub fn is_triggered(&self) -> bool {
        *self.rx.borrow()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use pretty_assertions::assert_eq;
    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn token_resolves_after_trigger() {
        let shutdown = Shutdown::new();
        let mut token = shutdown.token();
        assert!(!token.is_triggered());
        shutdown.trigger();
        // Generous timeout; resolves immediately in practice.
        timeout(Duration::from_millis(100), token.cancelled())
            .await
            .expect("token should resolve");
        assert!(token.is_triggered());
    }

    #[tokio::test]
    async fn token_resolves_immediately_if_already_triggered() {
        let shutdown = Shutdown::new();
        shutdown.trigger();
        let mut token = shutdown.token();
        timeout(Duration::from_millis(100), token.cancelled())
            .await
            .expect("token should resolve immediately");
    }

    #[tokio::test]
    async fn multiple_tokens_observe_one_trigger() {
        let shutdown = Shutdown::new();
        let mut a = shutdown.token();
        let mut b = shutdown.token();
        let mut c = shutdown.token();
        shutdown.trigger();
        timeout(Duration::from_millis(100), async move {
            a.cancelled().await;
            b.cancelled().await;
            c.cancelled().await;
        })
        .await
        .expect("all tokens should resolve");
    }

    #[tokio::test]
    async fn trigger_is_idempotent() {
        let shutdown = Shutdown::new();
        shutdown.trigger();
        shutdown.trigger();
        shutdown.trigger();
        assert!(shutdown.is_triggered());
    }

    #[tokio::test]
    async fn token_does_not_resolve_before_trigger() {
        let shutdown = Shutdown::new();
        let mut token = shutdown.token();
        let res = timeout(Duration::from_millis(20), token.cancelled()).await;
        assert!(res.is_err(), "token should not resolve before trigger");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn programmatic_trigger_aborts_the_signal_task() {
        // L10: the signal-listener task used to be detached and only
        // exited when a real OS signal fired. On the programmatic
        // `trigger()` path (test harness / embedders) it leaked a parked
        // task + two signal-stream registrations for the runtime's life.
        // `trigger()` must now abort it.
        let shutdown = Arc::new(Shutdown::new());
        shutdown.install_signal_handlers().unwrap();
        let handle = shutdown
            .signal_task_handle()
            .expect("a signal task was spawned");

        shutdown.trigger();

        // The task ends (aborted, or it ran trigger() and returned).
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !handle.is_finished() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "signal task must terminate after a programmatic trigger",
            );
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn shutdown_drop_resolves_outstanding_tokens() {
        // If the source is dropped, tokens still wake up — no use
        // hanging forever.
        let shutdown = Shutdown::new();
        let mut token = shutdown.token();
        drop(shutdown);
        timeout(Duration::from_millis(100), token.cancelled())
            .await
            .expect("token should resolve when source drops");
    }

    #[tokio::test]
    async fn select_pattern_preempts_long_work() {
        // The realistic usage: select between cancellation and work.
        let shutdown = Shutdown::new();
        let mut token = shutdown.token();
        let work = async {
            tokio::time::sleep(Duration::from_mins(1)).await;
            "should not happen"
        };

        shutdown.trigger();
        let outcome = tokio::select! {
            () = token.cancelled() => "cancelled",
            r = work => r,
        };
        assert_eq!(outcome, "cancelled");
    }
}
